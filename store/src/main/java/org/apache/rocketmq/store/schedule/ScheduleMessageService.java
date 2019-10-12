/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store.schedule;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.running.RunningStats;
import org.apache.rocketmq.store.ConsumeQueue;
import org.apache.rocketmq.store.ConsumeQueueExt;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.StorePathConfigHelper;


/**
 * 同事务消息类似，RocketMQ 定时消息 也是通过Topic替换，后台线程异步发送实现的。
 * 具体逻辑是通过 org.apache.rocketmq.store.schedule.ScheduleMessageService 实现的。
 * <p>
 * RocketMQ支持指定级别的消息延迟，默认为1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h。
 * RocketMQ消息重试以及定时消息均是通过定时任务实现的。
 * 重试消息以及定时消息在存入commitLog之前会判断重试次数，如果大于0，则会将消息的topic设置为 SCHEDULE_TOPIC_XXXX。
 * ScheduleMessageService 在实例化之后会对 SCHEDULE_TOPIC_XXXX 主题下的消息进行定时调度，从而实现定时投递。
 * <p>
 * ScheduleMessageService 的初始化是在 DefaultMessageStore 实现的，具体的调用链如下：
 * BrokerStartup
 * |-main
 * |-start
 * |-createBrokerController
 * |-BrokerController.initialize()
 * |-controller.start()
 * |-DefaultMessageStore.start()
 * |-new ScheduleMessageService(this)
 * |-scheduleMessageService.start()
 * <p>
 * 从调用链可以看出，当 broker 启动完成，
 * ScheduleMessageService就开始对定时消息进行调度。
 *
 * RocketMQ的定时消息实现方式，我们总结一下它的完整流程：
 *
 * 消息发送方发送消息，设置 delayLevel。
 * 如果 delayLevel 大于 0，表明是一条延时消息，broker 处理该消息，将消息的主题、队列id进行备份后，改变消息的主题为 SCHEDULE_TOPIC_XXXX，队列 id= 延迟级别 -1，将消息持久化。
 * 通过定时任务 ScheduleMessageService 对定时消息进行处理，每隔 1s 从上次拉取偏移量取出所有的消息进行处理
 * 从消费队列中解析出消息的物理偏移量，从而从 commitLog 中取出消息
 * 根据消息的属性重建消息，恢复消息的 topic、原队列 id，将消息的延迟级别属性 delayLevel 清除掉，再次保存到 commitLog 中
 * 将消息转发到原主题对应的消费队列中，此时消费者可以对该消息进行消费。
 */
public class ScheduleMessageService extends ConfigManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    public static final String SCHEDULE_TOPIC = "SCHEDULE_TOPIC_XXXX";
    private static final long FIRST_DELAY_TIME = 1000L;
    private static final long DELAY_FOR_A_WHILE = 100L;
    private static final long DELAY_FOR_A_PERIOD = 10000L;

    /**
     * delayLevelTable,记录了对延迟级别的解析结果，key=延迟级别(level)，value=对应延迟级别的毫秒数
     */
    private final ConcurrentMap<Integer, Long> delayLevelTable = new ConcurrentHashMap<Integer, Long>(32);

    /**
     * offsetTable, 延迟级别对应的消费进度，key=延迟级别，value=对应延迟级别下的消费进度
     */
    private final ConcurrentMap<Integer, Long> offsetTable = new ConcurrentHashMap<Integer, Long>(32);
    private final DefaultMessageStore defaultMessageStore;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private Timer timer;
    private MessageStore writeMessageStore;
    private int maxDelayLevel;

    public ScheduleMessageService(final DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
        this.writeMessageStore = defaultMessageStore;
    }

    public static int queueId2DelayLevel(final int queueId) {
        return queueId + 1;
    }

    public static int delayLevel2QueueId(final int delayLevel) {
        return delayLevel - 1;
    }

    /**
     * @param writeMessageStore the writeMessageStore to set
     */
    public void setWriteMessageStore(MessageStore writeMessageStore) {
        this.writeMessageStore = writeMessageStore;
    }

    public void buildRunningStats(HashMap<String, String> stats) {
        Iterator<Map.Entry<Integer, Long>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, Long> next = it.next();
            int queueId = delayLevel2QueueId(next.getKey());
            long delayOffset = next.getValue();
            long maxOffset = this.defaultMessageStore.getMaxOffsetInQueue(SCHEDULE_TOPIC, queueId);
            String value = String.format("%d,%d", delayOffset, maxOffset);
            String key = String.format("%s_%d", RunningStats.scheduleMessageOffset.name(), next.getKey());
            stats.put(key, value);
        }
    }

    private void updateOffset(int delayLevel, long offset) {
        this.offsetTable.put(delayLevel, offset);
    }

    public long computeDeliverTimestamp(final int delayLevel, final long storeTimestamp) {
        Long time = this.delayLevelTable.get(delayLevel);
        if (time != null) {
            return time + storeTimestamp;
        }

        return storeTimestamp + 1000;
    }

    /**
     * 该方法是延迟消息(定时消息)调度的核心逻辑。
     * start 方法的核心思想为 : 对不同的延迟级别创建对应的定时任务，通过定时任务对持久化的消息队列的进度进行存储。
     */
    public void start() {
        if (started.compareAndSet(false, true)) {
            this.timer = new Timer("ScheduleMessageTimerThread", true);

            // 首先对 delayLevelTable 进行迭代，取出每一个级别及其对应的延时长度。
            for (Map.Entry<Integer, Long> entry : this.delayLevelTable.entrySet()) {
                Integer level = entry.getKey();
                Long timeDelay = entry.getValue();
                Long offset = this.offsetTable.get(level);

                // 获取该级别对应的消费进度 offset，如果不存在则设置为 0
                if (null == offset) {
                    offset = 0L;
                }

                // 如果延时不为空，则延迟 1 秒执行定时任务
                if (timeDelay != null) {
                    this.timer.schedule(new DeliverDelayedMessageTimerTask(level, offset), FIRST_DELAY_TIME);
                }

                // 首先对 delayLevelTable 进行遍历，获取对应延迟级别 level 对应的消费进度，
                // 默认进度不存在，每个延迟级别对应的消费进度都从0开始。
                // 创建定时任务开始进行调度，每个定时任务初始都延迟1秒开始进行调度。后续则使用对应的延迟级别进行调度。
                // 注意：延时级别与消费队列的关系为：消息队列 id = 延时级别 -1，具体逻辑在 queueId2DelayLevel 方法中。
            }

            // 这段代码的核心逻辑为，执行定时任务，每隔10s进行一次消费进度的持久化操作。
            // 具体的持久化刷盘频率可以通过 flushDelayOffsetInterval 参数进行配置
            this.timer.scheduleAtFixedRate(new TimerTask() {

                @Override
                public void run() {
                    try {
                        if (started.get()) {
                            ScheduleMessageService.this.persist();
                        }
                    } catch (Throwable e) {
                        log.error("scheduleAtFixedRate flush exception", e);
                    }
                }
            }, 10000, this.defaultMessageStore.getMessageStoreConfig().getFlushDelayOffsetInterval());
        }
    }

    public void shutdown() {
        if (this.started.compareAndSet(true, false)) {
            if (null != this.timer) {
                this.timer.cancel();
            }
        }

    }

    public boolean isStarted() {
        return started.get();
    }

    public int getMaxDelayLevel() {
        return maxDelayLevel;
    }

    @Override
    public String encode() {
        return this.encode(false);
    }


    /**
     * load()方法的逻辑比较清晰，它的主要职责为：
     * <p>
     * 通过 super.load() 方法获取配置文件,加载延迟消息的消费进度
     * 初始化 delayLevelTable
     * RocketMQ 将延时消息的消费进度存储于 ${RocketMQ_Home}/store/config/delayOffset.json下
     *
     * @return
     */
    @Override
    public boolean load() {
        boolean result = super.load();
        result = result && this.parseDelayLevel();
        return result;
    }

    @Override
    public String configFilePath() {
        return StorePathConfigHelper.getDelayOffsetStorePath(this.defaultMessageStore.getMessageStoreConfig()
                .getStorePathRootDir());
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            DelayOffsetSerializeWrapper delayOffsetSerializeWrapper =
                    DelayOffsetSerializeWrapper.fromJson(jsonString, DelayOffsetSerializeWrapper.class);
            if (delayOffsetSerializeWrapper != null) {
                this.offsetTable.putAll(delayOffsetSerializeWrapper.getOffsetTable());
            }
        }
    }

    @Override
    public String encode(final boolean prettyFormat) {
        DelayOffsetSerializeWrapper delayOffsetSerializeWrapper = new DelayOffsetSerializeWrapper();
        delayOffsetSerializeWrapper.setOffsetTable(this.offsetTable);
        return delayOffsetSerializeWrapper.toJson(prettyFormat);
    }

    /**
     * 这段代码很好理解，就是对配置中的延时串通过空格进行分割为数组，
     * 按照下标及单位，计算得到每个等级对应的毫秒数，最终存放在 delayLevelTable 中
     * 实现 delayLevelTable 的初始化，便于后续在代码逻辑中进行使用。
     * <p>
     * 如果没有设置则使用代码中的默认值。
     *
     * @return
     */
    public boolean parseDelayLevel() {

        // 初始化一个时间单位 map，key 为秒、分、时、天；value为对应单位的毫秒数
        HashMap<String, Long> timeUnitTable = new HashMap<String, Long>();
        timeUnitTable.put("s", 1000L);
        timeUnitTable.put("m", 1000L * 60);
        timeUnitTable.put("h", 1000L * 60 * 60);
        timeUnitTable.put("d", 1000L * 60 * 60 * 24);

        // 从 defaultMessageStore 中获取配置文件，从配置文件中获取延迟级别配置串，即：messageDelayLevel
        String levelString = this.defaultMessageStore.getMessageStoreConfig().getMessageDelayLevel();
        try {

            // 根据空格进行拆分，分解为String数组
            String[] levelArray = levelString.split(" ");

            // 遍历String数组
            for (int i = 0; i < levelArray.length; i++) {
                String value = levelArray[i];
                String ch = value.substring(value.length() - 1);
                Long tu = timeUnitTable.get(ch);

                // key = 延迟级别，等于下标+1
                int level = i + 1;
                if (level > this.maxDelayLevel) {
                    this.maxDelayLevel = level;
                }
                long num = Long.parseLong(value.substring(0, value.length() - 1));

                // value = 单位对应毫秒数 * 解析得到的时间单位
                long delayTimeMillis = tu * num;

                // 存放到 delayLevelTable
                this.delayLevelTable.put(level, delayTimeMillis);
            }
        } catch (Exception e) {
            log.error("parseDelayLevel exception", e);
            log.info("levelString String = {}", levelString);
            return false;
        }

        return true;
    }

    /**
     * RocketMQ 对定时消息的每一个延迟级别都设置了一个定时任务，这个定时任务识通过 DeliverDelayedMessageTimerTask 实现的。
     */
    class DeliverDelayedMessageTimerTask extends TimerTask {
        private final int delayLevel;
        private final long offset;

        public DeliverDelayedMessageTimerTask(int delayLevel, long offset) {
            this.delayLevel = delayLevel;
            this.offset = offset;
        }

        @Override
        public void run() {
            try {
                if (isStarted()) {
                    this.executeOnTimeup();
                }
            } catch (Exception e) {
                // XXX: warn and notify me
                log.error("ScheduleMessageService, executeOnTimeup exception", e);
                ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(
                        this.delayLevel, this.offset), DELAY_FOR_A_PERIOD);
            }
        }

        /**
         * @return
         */
        private long correctDeliverTimestamp(final long now, final long deliverTimestamp) {

            long result = deliverTimestamp;

            long maxTimestamp = now + ScheduleMessageService.this.delayLevelTable.get(this.delayLevel);
            if (deliverTimestamp > maxTimestamp) {
                result = now;
            }

            return result;
        }

        public void executeOnTimeup() {

            // 首先根据 topic=SCHEDULE_TOPIC_XXXX，延迟级别转换为队列 id，查询到当前的消费队列。
            ConsumeQueue cq =
                    ScheduleMessageService.this.defaultMessageStore.findConsumeQueue(SCHEDULE_TOPIC, delayLevel2QueueId(delayLevel));

            long failScheduleOffset = offset;

            // 根据当前的 offset 从消费队列中获取当前所有的有效消息，如果未能获取到则更新拉取进度，等待定时任务下次进行尝试。
            if (cq != null) {
                SelectMappedBufferResult bufferCQ = cq.getIndexBuffer(this.offset);
                if (bufferCQ != null) {
                    try {
                        long nextOffset = offset;
                        int i = 0;
                        ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();

                        for (; i < bufferCQ.getSize(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                            long offsetPy = bufferCQ.getByteBuffer().getLong();
                            int sizePy = bufferCQ.getByteBuffer().getInt();
                            long tagsCode = bufferCQ.getByteBuffer().getLong();

                            if (cq.isExtAddr(tagsCode)) {
                                if (cq.getExt(tagsCode, cqExtUnit)) {
                                    tagsCode = cqExtUnit.getTagsCode();
                                } else {
                                    //can't find ext content.So re compute tags code.
                                    log.error("[BUG] can't find consume queue extend file content!addr={}, offsetPy={}, sizePy={}", tagsCode, offsetPy, sizePy);
                                    long msgStoreTime = defaultMessageStore.getCommitLog().pickupStoreTimestamp(offsetPy, sizePy);
                                    tagsCode = computeDeliverTimestamp(delayLevel, msgStoreTime);
                                }
                            }


                            // 定时任务每次执行到这里都进行时间比较，计算延迟时间与当前时间的差值，如果延迟时间-当前时间<=0说明该延迟消息应当被处理，使其能够被消费者消费。
                            long now = System.currentTimeMillis();
                            long deliverTimestamp = this.correctDeliverTimestamp(now, tagsCode);
                            nextOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);

                            long countdown = deliverTimestamp - now;

                            if (countdown <= 0) {
                                MessageExt msgExt =
                                        ScheduleMessageService.this.defaultMessageStore.lookMessageByOffset(offsetPy, sizePy);

                                // 根据消息偏移量及消息大小从 commitLog 中查询消息，如果查到，则开始执行正式的消息消费准备工作。
                                // 对消息执行重新存储操作，恢复原先的队列以及消息 topic，再将消息重新持久化到 commitLog 中，此时的消息已经能够被消费者拉取到。
                                if (msgExt != null) {
                                    try {
                                        MessageExtBrokerInner msgInner = this.messageTimeup(msgExt);

                                        // 将还原后的消息重新持久化到 commitLog 中。
                                        PutMessageResult putMessageResult = ScheduleMessageService.this.writeMessageStore.putMessage(msgInner);

                                        if (putMessageResult != null
                                                && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
                                            continue;
                                        } else {
                                            // XXX: warn and notify me
                                            log.error(
                                                    "ScheduleMessageService, a message time up, but reput it failed, topic: {} msgId {}",
                                                    msgExt.getTopic(), msgExt.getMsgId());
                                            ScheduleMessageService.this.timer.schedule(
                                                    new DeliverDelayedMessageTimerTask(this.delayLevel,
                                                            nextOffset), DELAY_FOR_A_PERIOD);
                                            ScheduleMessageService.this.updateOffset(this.delayLevel,
                                                    nextOffset);
                                            return;
                                        }
                                    } catch (Exception e) {
                                        log.error("ScheduleMessageService, messageTimeup execute error, drop it. msgExt=" + msgExt + ", nextOffset=" + nextOffset + ",offsetPy=" + offsetPy + ",sizePy=" + sizePy, e);
                                    }
                                }
                            } else {
                                ScheduleMessageService.this.timer.schedule(
                                        new DeliverDelayedMessageTimerTask(this.delayLevel, nextOffset),
                                        countdown);
                                ScheduleMessageService.this.updateOffset(this.delayLevel, nextOffset);
                                return;
                            }
                        }

                        // 更新当前延迟队列的消息拉取进度，继续处理后续的消息。
                        nextOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);
                        ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(this.delayLevel, nextOffset), DELAY_FOR_A_WHILE);
                        ScheduleMessageService.this.updateOffset(this.delayLevel, nextOffset);
                        return;
                    } finally {

                        bufferCQ.release();
                    }
                } // end of if (bufferCQ != null)
                else {

                    long cqMinOffset = cq.getMinOffsetInQueue();
                    if (offset < cqMinOffset) {
                        failScheduleOffset = cqMinOffset;
                        log.error("schedule CQ offset invalid. offset=" + offset + ", cqMinOffset="
                                + cqMinOffset + ", queueId=" + cq.getQueueId());
                    }
                }
            } // end of if (cq != null)

            ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(this.delayLevel,
                    failScheduleOffset), DELAY_FOR_A_WHILE);
        }

        /**
         * 恢复原消息主题及队列
         *
         * @param msgExt
         * @return
         */
        private MessageExtBrokerInner messageTimeup(MessageExt msgExt) {


            // 建立一个新的MessageExtBrokerInner实体
            MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
            msgInner.setBody(msgExt.getBody());
            msgInner.setFlag(msgExt.getFlag());
            MessageAccessor.setProperties(msgInner, msgExt.getProperties());

            TopicFilterType topicFilterType = MessageExt.parseTopicFilterType(msgInner.getSysFlag());
            long tagsCodeValue =
                    MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
            msgInner.setTagsCode(tagsCodeValue);
            msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));

            msgInner.setSysFlag(msgExt.getSysFlag());
            msgInner.setBornTimestamp(msgExt.getBornTimestamp());
            msgInner.setBornHost(msgExt.getBornHost());
            msgInner.setStoreHost(msgExt.getStoreHost());
            msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());

            msgInner.setWaitStoreMsgOK(false);

            // 清理消息延迟级别属性
            MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_DELAY_TIME_LEVEL);

            // 恢复消息原主题
            msgInner.setTopic(msgInner.getProperty(MessageConst.PROPERTY_REAL_TOPIC));

            // 恢复消息原队列id
            String queueIdStr = msgInner.getProperty(MessageConst.PROPERTY_REAL_QUEUE_ID);
            int queueId = Integer.parseInt(queueIdStr);
            msgInner.setQueueId(queueId);

            return msgInner;
        }
    }
}
