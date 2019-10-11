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
package org.apache.rocketmq.client.impl;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.RPCHook;

public class MQClientManager {
    private final static InternalLogger log = ClientLogger.getLog();
    private static MQClientManager instance = new MQClientManager();
    private AtomicInteger factoryIndexGenerator = new AtomicInteger();

    /**
     * clientId-MQClientInstance
     */
    private ConcurrentMap<String, MQClientInstance> factoryTable = new ConcurrentHashMap<>();

    private MQClientManager() {

    }

    public static MQClientManager getInstance() {
        return instance;
    }

    public MQClientInstance getAndCreateMQClientInstance(final ClientConfig clientConfig) {
        return getAndCreateMQClientInstance(clientConfig, null);
    }

    /**
     * 对于同一个JVM中的不同消费者和不同生产者在启动时候获取到的MQClientInstance是同一个。
     * MQClientInstance是封装了网络调用相关的逻辑。
     *
     * @param clientConfig
     * @param rpcHook
     * @return
     */
    public MQClientInstance getAndCreateMQClientInstance(final ClientConfig clientConfig, RPCHook rpcHook) {

        // 构建MQClientId clientId=客户端ip+@+实例名+unitName（可选）
        String clientId = clientConfig.buildMQClientId();

        // 从 clientId 与 MQClientInstance 映射表 factoryTable 中获取当前 clientId 对应的 MQClientInstance
        MQClientInstance instance = this.factoryTable.get(clientId);

        // 如果 MQClientInstance 不存在则创建一个新的并放入映射表 factoryTable 中
        if (null == instance) {
            instance = new MQClientInstance(clientConfig.cloneClientConfig(), this.factoryIndexGenerator.getAndIncrement(), clientId, rpcHook);
            MQClientInstance prev = this.factoryTable.putIfAbsent(clientId, instance);
            if (prev != null) {
                instance = prev;
                log.warn("Returned Previous MQClientInstance for clientId:[{}]", clientId);
            } else {
                log.info("Created new MQClientInstance for clientId:[{}]", clientId);
            }
        }

        return instance;
    }

    public void removeClientFactory(final String clientId) {
        this.factoryTable.remove(clientId);
    }
}
