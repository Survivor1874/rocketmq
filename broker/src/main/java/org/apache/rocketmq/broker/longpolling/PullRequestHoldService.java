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
package org.apache.rocketmq.broker.longpolling;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.SystemClock;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.ConsumeQueueExt;

public class PullRequestHoldService extends ServiceThread {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private static final String TOPIC_QUEUEID_SEPARATOR = "@";
    private final BrokerController brokerController;
    private final SystemClock systemClock = new SystemClock();
    private ConcurrentMap<String/* topic@queueId */, ManyPullRequest> pullRequestTable =
            new ConcurrentHashMap<String, ManyPullRequest>(1024);

    public PullRequestHoldService(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public void suspendPullRequest(final String topic, final int queueId, final PullRequest pullRequest) {
        String key = this.buildKey(topic, queueId);
        ManyPullRequest mpr = this.pullRequestTable.get(key);
        if (null == mpr) {
            mpr = new ManyPullRequest();
            ManyPullRequest prev = this.pullRequestTable.putIfAbsent(key, mpr);
            if (prev != null) {
                mpr = prev;
            }
        }

        // 将等待检测的 pullRequest 添加到 ManyPullRequest 中
        // 注意，这里的 ManyPullRequest 对象实际上是一组 PullRequest 的集合，
        // 它封装了一个 topic + queueId 下的一批消息。
        mpr.addPullRequest(pullRequest);
    }

    private String buildKey(final String topic, final int queueId) {
        StringBuilder sb = new StringBuilder();
        sb.append(topic);
        sb.append(TOPIC_QUEUEID_SEPARATOR);
        sb.append(queueId);
        return sb.toString();
    }

    /**
     * run方法不断检测被hold住的请求，它不断检查是否有消息获取成功。检测方法通过执行方法suspendPullRequest实现
     */
    @Override
    public void run() {
        log.info("{} service started", this.getServiceName());
        while (!this.isStopped()) {
            try {

                // 如果支持长轮询，则等待5秒
                if (this.brokerController.getBrokerConfig().isLongPollingEnable()) {
                    this.waitForRunning(5 * 1000);
                } else {

                    // 短轮询则默认等待1s
                    this.waitForRunning(this.brokerController.getBrokerConfig().getShortPollingTimeMills());
                }

                long beginLockTimestamp = this.systemClock.now();

                // 检测 hold 请求
                this.checkHoldRequest();
                long costTime = this.systemClock.now() - beginLockTimestamp;

                // 如果检测花费时间超过 5s 打印日志
                if (costTime > 5 * 1000) {
                    log.info("[NOTIFYME] check hold request cost {} ms.", costTime);
                }
            } catch (Throwable e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        log.info("{} service end", this.getServiceName());
    }

    @Override
    public String getServiceName() {
        return PullRequestHoldService.class.getSimpleName();
    }

    /**
     * checkHoldRequest()方法解析pullRequestTable的keySet，
     * 对key进行解析，取出topic及queueId，获取topic+queueId对应的当前MessageQueue的最大offset，
     * 并与当前的offset对比从而确定是否有新消息到达，具体逻辑在notifyMessageArriving(topic, queueId, offset);
     * 这里的检测逻辑整体是异步的，后台检测线程PullRequestHoldService一直在运行；
     * 在PullMessageProcessor中提交待检测的PullRequest到PullRequestHoldService，
     * 将其放入pullRequestTable，等待被PullRequestHoldService进行处理
     */
    private void checkHoldRequest() {

        // 迭代PullRequest Map，key=topic@queueId
        for (String key : this.pullRequestTable.keySet()) {

            // 解析出topic  queueId
            String[] kArray = key.split(TOPIC_QUEUEID_SEPARATOR);
            if (2 == kArray.length) {
                String topic = kArray[0];
                int queueId = Integer.parseInt(kArray[1]);

                // 获取当前获取的数据的最大offset
                final long offset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
                try {

                    // 通知消息到达
                    this.notifyMessageArriving(topic, queueId, offset);
                } catch (Throwable e) {
                    log.error("check hold request failed. topic={}, queueId={}", topic, queueId, e);
                }
            }
        }
    }

    /**
     * notifyMessageArriving主要作用为判断消息是否到来，并根据判断结果对客户端进行相应。
     * <p>
     * 比较maxOffset与当前的offset，如果当前最新offset大于请求offset，也就是有新消息到来，则将新消息返回给客户端
     * 校验是否超时，如果当前时间 >= 请求超时时间+hold阻塞时间，则返回客户端消息未找到
     * 该方法会在PullRequestHoldService中循环调用进行检查，也会在DefaultMessageStore中消息被存储的时候调用。
     * 这里体现了主动检查与被动通知共同作用的思路。
     * 当服务端处理完成之后，相应客户端，客户端会在消息处理完成之后
     * 再次将拉取请求pullRequest放到PullMessageService中，等待下次轮询。
     * 这样就能够一直进行消息拉取操作。
     *
     * @param topic
     * @param queueId
     * @param maxOffset
     */
    public void notifyMessageArriving(final String topic, final int queueId, final long maxOffset) {
        notifyMessageArriving(topic, queueId, maxOffset, null, 0, null, null);
    }

    public void notifyMessageArriving(final String topic, final int queueId, final long maxOffset, final Long tagsCode, long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
        String key = this.buildKey(topic, queueId);
        ManyPullRequest mpr = this.pullRequestTable.get(key);
        if (mpr != null) {

            // 根据key=topic@queueId从pullRequestTable获取ManyPullRequest
            // 如果ManyPullRequest不为空，拷贝ManyPullRequest中的List<PullRequest>
            List<PullRequest> requestList = mpr.cloneListAndClear();
            if (requestList != null) {

                // 构造响应list
                List<PullRequest> replayList = new ArrayList<PullRequest>();

                for (PullRequest request : requestList) {
                    long newestOffset = maxOffset;

                    // 如果当前最新的offset小于等于请求的offset
                    if (newestOffset <= request.getPullFromThisOffset()) {

                        // 当前最新的offset就是队列的最大offset
                        newestOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
                    }

                    // 如果当前最新offset大于请求offset，也就是有新消息到来
                    if (newestOffset > request.getPullFromThisOffset()) {

                        // 判断消息是否满足过滤表达式
                        boolean match = request.getMessageFilter().isMatchedByConsumeQueue(tagsCode,
                                new ConsumeQueueExt.CqExtUnit(tagsCode, msgStoreTime, filterBitMap));
                        // match by bit map, need eval again when properties is not null.
                        if (match && properties != null) {
                            match = request.getMessageFilter().isMatchedByCommitLog(null, properties);
                        }

                        if (match) {
                            try {

                                // 消息匹配，则将消息返回客户端
                                this.brokerController.getPullMessageProcessor().executeRequestWhenWakeup(request.getClientChannel(),
                                        request.getRequestCommand());
                            } catch (Throwable e) {
                                log.error("execute request when wakeup failed.", e);
                            }
                            continue;
                        }
                    }

                    // 判断是否超时
                    if (System.currentTimeMillis() >= (request.getSuspendTimestamp() + request.getTimeoutMillis())) {
                        try {

                            // 如果当前时间 >= 请求超时时间+hold时间，则返回客户端消息未找到
                            this.brokerController.getPullMessageProcessor().executeRequestWhenWakeup(request.getClientChannel(),
                                    request.getRequestCommand());
                        } catch (Throwable e) {
                            log.error("execute request when wakeup failed.", e);
                        }
                        continue;
                    }

                    replayList.add(request);
                }

                if (!replayList.isEmpty()) {
                    mpr.addPullRequest(replayList);
                }
            }
        }
    }
}
