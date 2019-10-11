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
package org.apache.rocketmq.client.impl.consumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeReturnType;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.CMResult;
import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.remoting.common.RemotingHelper;

public class ConsumeMessageConcurrentlyService implements ConsumeMessageService {

    private static final InternalLogger log = ClientLogger.getLog();

    // 消费推模式实现类
    private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;

    // 消费者引用
    private final DefaultMQPushConsumer defaultMQPushConsumer;

    // 并发消息事件监听回调
    private final MessageListenerConcurrently messageListener;

    // 消息消费任务队列
    private final BlockingQueue<Runnable> consumeRequestQueue;

    // 消息消费线程池
    private final ThreadPoolExecutor consumeExecutor;

    // 消息消费组
    private final String consumerGroup;

    // 添加消费任务到consumeExecutor定时调度器
    private final ScheduledExecutorService scheduledExecutorService;

    // 定时删除过期任务线程池
    private final ScheduledExecutorService cleanExpireMsgExecutors;

    public ConsumeMessageConcurrentlyService(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl, MessageListenerConcurrently messageListener) {

        // 初始化 defaultMQPushConsumerImpl，messageListener
        // 本地引用指向外部具体实现
        this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
        this.messageListener = messageListener;

        this.defaultMQPushConsumer = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer();

        // 消费者组
        this.consumerGroup = this.defaultMQPushConsumer.getConsumerGroup();

        // 初始化消费请求队列为LinkedBlockingQueue无界队列
        this.consumeRequestQueue = new LinkedBlockingQueue<Runnable>();

        // 初始化线程池，指向消费调度线程池
        this.consumeExecutor = new ThreadPoolExecutor(
                this.defaultMQPushConsumer.getConsumeThreadMin(),
                this.defaultMQPushConsumer.getConsumeThreadMax(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                this.consumeRequestQueue,
                new ThreadFactoryImpl("ConsumeMessageThread_"));

        // 初始化消费定时任务线程池，线程数=1
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ConsumeMessageScheduledThread_"));

        // 初始化清除过期消息线程池，线程数=1
        this.cleanExpireMsgExecutors = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("CleanExpireMsgScheduledThread_"));
    }

    /**
     * start方法是对 cleanExpireMsgExecutors 进行处理，开启清除过期消息的调度过程。
     */
    @Override
    public void start() {
        this.cleanExpireMsgExecutors.scheduleAtFixedRate(new Runnable() {
                                                             @Override
                                                             public void run() {
                                                                 cleanExpireMsg();
                                                             }
                                                         },

                // 15min 消费超时
                this.defaultMQPushConsumer.getConsumeTimeout(),
                this.defaultMQPushConsumer.getConsumeTimeout(),
                TimeUnit.MINUTES);
    }

    @Override
    public void shutdown() {
        this.scheduledExecutorService.shutdown();
        this.consumeExecutor.shutdown();
        this.cleanExpireMsgExecutors.shutdown();
    }

    @Override
    public void updateCorePoolSize(int corePoolSize) {
        if (corePoolSize > 0
                && corePoolSize <= Short.MAX_VALUE
                && corePoolSize < this.defaultMQPushConsumer.getConsumeThreadMax()) {
            this.consumeExecutor.setCorePoolSize(corePoolSize);
        }
    }

    @Override
    public void incCorePoolSize() {
        // long corePoolSize = this.consumeExecutor.getCorePoolSize();
        // if (corePoolSize < this.defaultMQPushConsumer.getConsumeThreadMax())
        // {
        // this.consumeExecutor.setCorePoolSize(this.consumeExecutor.getCorePoolSize()
        // + 1);
        // }
        // log.info("incCorePoolSize Concurrently from {} to {}, ConsumerGroup:
        // {}",
        // corePoolSize,
        // this.consumeExecutor.getCorePoolSize(),
        // this.consumerGroup);
    }

    @Override
    public void decCorePoolSize() {
        // long corePoolSize = this.consumeExecutor.getCorePoolSize();
        // if (corePoolSize > this.defaultMQPushConsumer.getConsumeThreadMin())
        // {
        // this.consumeExecutor.setCorePoolSize(this.consumeExecutor.getCorePoolSize()
        // - 1);
        // }
        // log.info("decCorePoolSize Concurrently from {} to {}, ConsumerGroup:
        // {}",
        // corePoolSize,
        // this.consumeExecutor.getCorePoolSize(),
        // this.consumerGroup);
    }

    @Override
    public int getCorePoolSize() {
        return this.consumeExecutor.getCorePoolSize();
    }

    @Override
    public ConsumeMessageDirectlyResult consumeMessageDirectly(MessageExt msg, String brokerName) {
        ConsumeMessageDirectlyResult result = new ConsumeMessageDirectlyResult();
        result.setOrder(false);
        result.setAutoCommit(true);

        List<MessageExt> msgs = new ArrayList<MessageExt>();
        msgs.add(msg);
        MessageQueue mq = new MessageQueue();
        mq.setBrokerName(brokerName);
        mq.setTopic(msg.getTopic());
        mq.setQueueId(msg.getQueueId());

        ConsumeConcurrentlyContext context = new ConsumeConcurrentlyContext(mq);

        this.resetRetryTopic(msgs);

        final long beginTime = System.currentTimeMillis();

        log.info("consumeMessageDirectly receive new message: {}", msg);

        try {
            ConsumeConcurrentlyStatus status = this.messageListener.consumeMessage(msgs, context);
            if (status != null) {
                switch (status) {
                    case CONSUME_SUCCESS:
                        result.setConsumeResult(CMResult.CR_SUCCESS);
                        break;
                    case RECONSUME_LATER:
                        result.setConsumeResult(CMResult.CR_LATER);
                        break;
                    default:
                        break;
                }
            } else {
                result.setConsumeResult(CMResult.CR_RETURN_NULL);
            }
        } catch (Throwable e) {
            result.setConsumeResult(CMResult.CR_THROW_EXCEPTION);
            result.setRemark(RemotingHelper.exceptionSimpleDesc(e));

            log.warn(String.format("consumeMessageDirectly exception: %s Group: %s Msgs: %s MQ: %s",
                    RemotingHelper.exceptionSimpleDesc(e),
                    ConsumeMessageConcurrentlyService.this.consumerGroup,
                    msgs,
                    mq), e);
        }

        result.setSpentTimeMills(System.currentTimeMillis() - beginTime);

        log.info("consumeMessageDirectly Result: {}", result);

        return result;
    }

    /**
     * ConsumeMessageConcurrentlyService 的消息消费过程主要方法为 submitConsumeRequest。
     * <p>
     * 通过 submitConsumeRequest 提交消费请求进行消费过程。
     *
     * @param msgs
     * @param processQueue
     * @param messageQueue
     * @param dispatchToConsume
     */
    @Override
    public void submitConsumeRequest(

            // 消息列表 默认一次从服务端拉取最多 32 条消息
            final List<MessageExt> msgs,

            // 消息处理队列
            final ProcessQueue processQueue,

            // 消息所属的消息队列
            final MessageQueue messageQueue,

            // 是否转发到消费线程池 并发消费时忽略该参数
            final boolean dispatchToConsume) {

        // 获取批量消费数量，这个值为 ConsumeMessageBatchMaxSize，默认为 1
        final int consumeBatchSize = this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize();

        // 如果消息的大小小于等于 consumeBatchSize，组装消费请求，提交到消费线程池中进行消费操作。
        // 如果异常则稍后再次提交消费请求，通过方法 submitConsumeRequestLater 实现。
        if (msgs.size() <= consumeBatchSize) {

            // 拉取的消息小于等于consumeBatchSize（默认为1）
            // 提交消费请求到线程池中进行消费
            ConsumeRequest consumeRequest = new ConsumeRequest(msgs, processQueue, messageQueue);
            try {
                this.consumeExecutor.submit(consumeRequest);
            } catch (RejectedExecutionException e) {
                this.submitConsumeRequestLater(consumeRequest);
            }

            // 如果拉取的消息条数大于consumeBatchSize，则对拉取到消息进行分页处理；
            // 每页大小为：consumeBatchSize。
            // 通过循环迭代的方式，创建多个ConsumeRequest消费请求任务，提交到消费线程池中。
        } else {

            // 拉取的消息大于 consumeBatchSize 进行分页提交任务到线程池
            for (int total = 0; total < msgs.size(); ) {
                List<MessageExt> msgThis = new ArrayList<MessageExt>(consumeBatchSize);
                for (int i = 0; i < consumeBatchSize; i++, total++) {
                    if (total < msgs.size()) {
                        msgThis.add(msgs.get(total));
                    } else {
                        break;
                    }
                }

                ConsumeRequest consumeRequest = new ConsumeRequest(msgThis, processQueue, messageQueue);
                try {
                    // 提交消费任务到消费线程池
                    this.consumeExecutor.submit(consumeRequest);

                    // 如果触发拒绝提交异常，则稍后继续提交。
                    // 实际上，由于任务队列是 LinkedBlockingQueue 无界队列，因此理论上不会出现拒绝提交。
                } catch (RejectedExecutionException e) {
                    for (; total < msgs.size(); total++) {
                        msgThis.add(msgs.get(total));
                    }

                    this.submitConsumeRequestLater(consumeRequest);
                }
            }
        }
    }

    public void resetRetryTopic(final List<MessageExt> msgs) {
        final String groupTopic = MixAll.getRetryTopic(consumerGroup);
        for (MessageExt msg : msgs) {
            String retryTopic = msg.getProperty(MessageConst.PROPERTY_RETRY_TOPIC);
            if (retryTopic != null && groupTopic.equals(msg.getTopic())) {
                msg.setTopic(retryTopic);
            }
        }
    }

    /**
     * cleanExpireMsg 方法定时对 ProcessQueue 进行处理，将其中的消息进行清理
     */
    private void cleanExpireMsg() {
        Iterator<Map.Entry<MessageQueue, ProcessQueue>> it = this.defaultMQPushConsumerImpl.getRebalanceImpl().getProcessQueueTable().entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<MessageQueue, ProcessQueue> next = it.next();
            ProcessQueue pq = next.getValue();
            pq.cleanExpiredMsg(this.defaultMQPushConsumer);
        }
    }

    /**
     * 消费进度offset进行处理。
     *
     * @param status
     * @param context
     * @param consumeRequest
     */
    public void processConsumeResult(

            // 并行消费结果
            final ConsumeConcurrentlyStatus status,

            // 并行消费上下文
            final ConsumeConcurrentlyContext context,

            // 消费请求
            final ConsumeRequest consumeRequest
    ) {

        int ackIndex = context.getAckIndex();

        if (consumeRequest.getMsgs().isEmpty()) {
            return;
        }

        // 判断消费结果，如果是 CONSUME_SUCCESS 则设置 ackIndex=msgs.size() - 1;
        // 如果是 RECONSUME_LATER 则设置 ackIndex=-1。为发送消息确认ACK做准备。
        switch (status) {
            case CONSUME_SUCCESS:
                if (ackIndex >= consumeRequest.getMsgs().size()) {
                    ackIndex = consumeRequest.getMsgs().size() - 1;
                }
                int ok = ackIndex + 1;
                int failed = consumeRequest.getMsgs().size() - ok;
                this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), ok);
                this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), failed);
                break;
            case RECONSUME_LATER:
                ackIndex = -1;
                this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(),
                        consumeRequest.getMsgs().size());
                break;
            default:
                break;
        }

        // 根据消费类型，进行处理，如果是广播模式：业务侧返回RECONSUME_LATER不会重新消费，只会打印告警日志；
        // 如果是集群模式，消息消费成功不执行sendMessageBack；当业务侧返回RECONSUME_LATER时，这批消息需要将ACK发送给broker。
        // 需要将它们重新封装为consumeRequest，延迟五秒后重新消费。
        switch (this.defaultMQPushConsumer.getMessageModel()) {
            case BROADCASTING:
                for (int i = ackIndex + 1; i < consumeRequest.getMsgs().size(); i++) {
                    MessageExt msg = consumeRequest.getMsgs().get(i);
                    log.warn("BROADCASTING, the message consume failed, drop it, {}", msg.toString());
                }
                break;
            case CLUSTERING:
                List<MessageExt> msgBackFailed = new ArrayList<MessageExt>(consumeRequest.getMsgs().size());
                for (int i = ackIndex + 1; i < consumeRequest.getMsgs().size(); i++) {
                    MessageExt msg = consumeRequest.getMsgs().get(i);
                    boolean result = this.sendMessageBack(msg, context);
                    if (!result) {
                        msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
                        msgBackFailed.add(msg);
                    }
                }

                if (!msgBackFailed.isEmpty()) {
                    consumeRequest.getMsgs().removeAll(msgBackFailed);

                    this.submitConsumeRequestLater(msgBackFailed, consumeRequest.getProcessQueue(), consumeRequest.getMessageQueue());
                }
                break;
            default:
                break;
        }

        // 从ProcessQueue中将这批成功消费的消息移除，通过offset更新消费进度；
        // 以便后续能够从上次的消费位点继续消费，避免重复消费。
        long offset = consumeRequest.getProcessQueue().removeMessage(consumeRequest.getMsgs());
        if (offset >= 0 && !consumeRequest.getProcessQueue().isDropped()) {
            this.defaultMQPushConsumerImpl.getOffsetStore().updateOffset(consumeRequest.getMessageQueue(), offset, true);
        }
    }

    public ConsumerStatsManager getConsumerStatsManager() {
        return this.defaultMQPushConsumerImpl.getConsumerStatsManager();
    }

    public boolean sendMessageBack(final MessageExt msg, final ConsumeConcurrentlyContext context) {
        int delayLevel = context.getDelayLevelWhenNextConsume();

        try {
            this.defaultMQPushConsumerImpl.sendMessageBack(msg, delayLevel, context.getMessageQueue().getBrokerName());
            return true;
        } catch (Exception e) {
            log.error("sendMessageBack exception, group: " + this.consumerGroup + " msg: " + msg.toString(), e);
        }

        return false;
    }

    /**
     * 这个方法中调用了 submitConsumeRequest 进行了消息消费处理。
     * 这样我们的消费流程就完美的闭环了。
     *
     * @param msgs
     * @param processQueue
     * @param messageQueue
     */
    private void submitConsumeRequestLater(
            final List<MessageExt> msgs,
            final ProcessQueue processQueue,
            final MessageQueue messageQueue
    ) {

        this.scheduledExecutorService.schedule(new Runnable() {

            @Override
            public void run() {
                ConsumeMessageConcurrentlyService.this.submitConsumeRequest(msgs, processQueue, messageQueue, true);
            }
        }, 5000, TimeUnit.MILLISECONDS);
    }

    /**
     * 通过scheduledExecutorService进行调度，每5秒再次提交一次消息消费请求。
     * submit接受一个Runnable接口实现，也就是这里的 ConsumeRequest；
     * 通过调用该Runnable的run方法实现具体的调度逻辑。
     *
     * @param consumeRequest
     */
    private void submitConsumeRequestLater(final ConsumeRequest consumeRequest) {

        this.scheduledExecutorService.schedule(new Runnable() {

            @Override
            public void run() {
                ConsumeMessageConcurrentlyService.this.consumeExecutor.submit(consumeRequest);
            }
        }, 5000, TimeUnit.MILLISECONDS);
    }

    class ConsumeRequest implements Runnable {
        private final List<MessageExt> msgs;
        private final ProcessQueue processQueue;
        private final MessageQueue messageQueue;

        public ConsumeRequest(List<MessageExt> msgs, ProcessQueue processQueue, MessageQueue messageQueue) {
            this.msgs = msgs;
            this.processQueue = processQueue;
            this.messageQueue = messageQueue;
        }

        public List<MessageExt> getMsgs() {
            return msgs;
        }

        public ProcessQueue getProcessQueue() {
            return processQueue;
        }

        @Override
        public void run() {

            // 首先检查processQueue的dropped是否为true，如果是true，则停止消费，直接return。
            // 当发生消息 rebalance 时，会设置 dropped==true，这么做的目的是防止消费者消费不属于自己的消息队列。
            if (this.processQueue.isDropped()) {
                log.info("the message queue not be able to consume, because it's dropped. group={} {}", ConsumeMessageConcurrentlyService.this.consumerGroup, this.messageQueue);
                return;
            }

            MessageListenerConcurrently listener = ConsumeMessageConcurrentlyService.this.messageListener;
            ConsumeConcurrentlyContext context = new ConsumeConcurrentlyContext(messageQueue);
            ConsumeConcurrentlyStatus status = null;

            ConsumeMessageContext consumeMessageContext = null;

            // 如果消费者存在钩子函数，则通过 executeHookBefore 调用该钩子函数
            if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                consumeMessageContext = new ConsumeMessageContext();
                consumeMessageContext.setConsumerGroup(defaultMQPushConsumer.getConsumerGroup());
                consumeMessageContext.setProps(new HashMap<String, String>());
                consumeMessageContext.setMq(messageQueue);
                consumeMessageContext.setMsgList(msgs);
                consumeMessageContext.setSuccess(false);
                ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.executeHookBefore(consumeMessageContext);
            }

            // 此处的代码是消费的核心部分。
            // 首先判断msgs是否为空，如果不为空，则迭代msgs，设置消费开始时间戳，
            // 回调客户端实现的 MessageListenerConcurrently.consumeMessage 方法执行具体消费逻辑，获得其消费结果status。
            long beginTimestamp = System.currentTimeMillis();
            boolean hasException = false;
            ConsumeReturnType returnType = ConsumeReturnType.SUCCESS;
            try {
                ConsumeMessageConcurrentlyService.this.resetRetryTopic(msgs);
                if (msgs != null && !msgs.isEmpty()) {
                    for (MessageExt msg : msgs) {
                        MessageAccessor.setConsumeStartTimeStamp(msg, String.valueOf(System.currentTimeMillis()));
                    }
                }

                // 通过Collections.unmodifiableList将msgs包装为不可修改的视图
                status = listener.consumeMessage(Collections.unmodifiableList(msgs), context);

                // 如果消费执行异常则hasException = true;
            } catch (Throwable e) {
                log.warn("consumeMessage exception: {} Group: {} Msgs: {} MQ: {}",
                        RemotingHelper.exceptionSimpleDesc(e),
                        ConsumeMessageConcurrentlyService.this.consumerGroup,
                        msgs,
                        messageQueue);
                hasException = true;
            }

            // 计算消费耗时
            long consumeRT = System.currentTimeMillis() - beginTimestamp;

            // 根据具体的status返回值进行后续处理：
            // 如果status为空，且hasException==true，则返回ConsumeReturnType.EXCEPTION，
            // 否则返回 ConsumeReturnType.RETURNNULL
            if (null == status) {
                if (hasException) {
                    returnType = ConsumeReturnType.EXCEPTION;
                } else {
                    returnType = ConsumeReturnType.RETURNNULL;
                }

                // 消费超时
            } else if (consumeRT >= defaultMQPushConsumer.getConsumeTimeout() * 60 * 1000) {
                returnType = ConsumeReturnType.TIME_OUT;

                // 业务侧返回RECONSUME_LATER，需要重新消费，returnType为消费失败
            } else if (ConsumeConcurrentlyStatus.RECONSUME_LATER == status) {
                returnType = ConsumeReturnType.FAILED;

                // 业务侧返回CONSUME_SUCCESS，消费成功，returnType为消费成功
            } else if (ConsumeConcurrentlyStatus.CONSUME_SUCCESS == status) {
                returnType = ConsumeReturnType.SUCCESS;
            }

            if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                consumeMessageContext.getProps().put(MixAll.CONSUME_CONTEXT_TYPE, returnType.name());
            }

            // 如果客户端返回的status为null，则赋值为RECONSUME_LATER，以便重复消费。
            if (null == status) {
                log.warn("consumeMessage return null, Group: {} Msgs: {} MQ: {}",
                        ConsumeMessageConcurrentlyService.this.consumerGroup,
                        msgs,
                        messageQueue);
                status = ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }

            // 如果存在钩子函数，则执行钩子函数executeHookAfter
            if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                consumeMessageContext.setStatus(status.toString());
                consumeMessageContext.setSuccess(ConsumeConcurrentlyStatus.CONSUME_SUCCESS == status);
                ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.executeHookAfter(consumeMessageContext);
            }

            ConsumeMessageConcurrentlyService.this.getConsumerStatsManager()
                    .incConsumeRT(ConsumeMessageConcurrentlyService.this.consumerGroup, messageQueue.getTopic(), consumeRT);

            // 执行消费逻辑之后，再次判断processQueue的dropped状态；
            // 如果为true，则不进行任何处理；
            // 当非true时，调用processConsumeResult对消费结果进行处理
            // 之所以当processQueue的dropped状态为true时不做任何处理，
            // 是因为当processQueue.dropped==true时，
            // 说明此时可能出现了新消费者的加入/原消费者down机等情况，导致原先消费者的队列在rebalance之后分配给了新的消费者。
            // 那么，这部分消息会被重新消费，因此此处就不需要做多余的处理，等待重新消费就可以了。
            if (!processQueue.isDropped()) {
                ConsumeMessageConcurrentlyService.this.processConsumeResult(status, context, this);
            } else {
                log.warn("processQueue is dropped without process consume result. messageQueue={}, msgs={}", messageQueue, msgs);
            }
        }

        public MessageQueue getMessageQueue() {
            return messageQueue;
        }

    }
}
