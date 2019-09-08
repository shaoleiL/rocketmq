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

package org.apache.rocketmq.client.latency;

import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;

public class MQFaultStrategy {
    private final static InternalLogger log = ClientLogger.getLog();

    // 延迟机制接口
    private final LatencyFaultTolerance<String> latencyFaultTolerance = new LatencyFaultToleranceImpl();

    private boolean sendLatencyFaultEnable = false;

    // latencyMax,最大延迟时间数值，再消息发送之前，先记录当前时间（start），然后消息发送成功或失败时记录当前时间（end），(end-start)代表一次消息延迟时间，
    // 发送错误时，updateFaultItem中isolation为真，与latencyMax中值进行比较时得值为30s,也就时该broker在接下来得600000L，也就时5分钟内不提供服务，等待该Broker的恢复。
    /**
     * latencyMax，根据FaultItem.currentLatency本次消息发送延迟，从latencyMax尾部向前找到
     * 第一个比currentLatency小的索引index,如果没有找到，返回0。然后根据这个索引从
     * notAvailableDuration数组中取出对应的时间，在这个时长内，Broker 将设置为不可用。
     */
    private long[] latencyMax = {50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L};
    private long[] notAvailableDuration = {0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L};

    public long[] getNotAvailableDuration() {
        return notAvailableDuration;
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.notAvailableDuration = notAvailableDuration;
    }

    public long[] getLatencyMax() {
        return latencyMax;
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.latencyMax = latencyMax;
    }

    public boolean isSendLatencyFaultEnable() {
        return sendLatencyFaultEnable;
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.sendLatencyFaultEnable = sendLatencyFaultEnable;
    }

    /**
     * Broker故障延迟机制，选择一个MessageQueue
     */
    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {
        // 是否开启消息失败延迟，该值在消息发送者那里可以设置，
        // 如果该值为false,直接从topic的所有队列中选择下一个，而不考虑该消息队列是否可用（比如Broker挂掉）
        if (this.sendLatencyFaultEnable) {
            try {
                // @1-start ~ end,这里使用了本地线程变量ThreadLocal保存上一次发送的消息队列下标，消息发送使用轮询机制获取下一个发送消息队列。
                int index = tpInfo.getSendWhichQueue().getAndIncrement();  // @1 start
                for (int i = 0; i < tpInfo.getMessageQueueList().size(); i++) {
                    int pos = Math.abs(index++) % tpInfo.getMessageQueueList().size();
                    if (pos < 0)
                        pos = 0;
                    MessageQueue mq = tpInfo.getMessageQueueList().get(pos);  // @1 end
                    //从@1~@2，一旦一个MessageQueue符合条件，即刻返回，但该Topic所在的所有Broker全部标记不可用时，进入到下一步逻辑处理。
                    // （在此处，我们要知道，标记为不可用，并不代表真的不可用，Broker是可用在故障期间被运营管理人员进行恢复的，比如重启）
                    // 判断当前的消息队列是否可用
                    if (latencyFaultTolerance.isAvailable(mq.getBrokerName())) { // @2
                        if (null == lastBrokerName || mq.getBrokerName().equals(lastBrokerName))
                            return mq;
                    }
                }

                // 代码@4，5，就是根据Broker的startTimestart进行一个排序，值越小，排前面，然后再选择一个，返回
                // （此时不能保证一定可用，会抛出异常，如果消息发送方式是同步调用，则有重试机制）
                final String notBestBroker = latencyFaultTolerance.pickOneAtLeast(); //@4
                int writeQueueNums = tpInfo.getQueueIdByBroker(notBestBroker); // @5
                if (writeQueueNums > 0) {
                    final MessageQueue mq = tpInfo.selectOneMessageQueue();
                    if (notBestBroker != null) {
                        mq.setBrokerName(notBestBroker);
                        mq.setQueueId(tpInfo.getSendWhichQueue().getAndIncrement() % writeQueueNums);
                    }
                    return mq;
                } else {
                    latencyFaultTolerance.remove(notBestBroker);
                }
            } catch (Exception e) {
                log.error("Error occurred when selecting message queue", e);
            }

            return tpInfo.selectOneMessageQueue();
        }

        return tpInfo.selectOneMessageQueue(lastBrokerName);
    }

    /**
     * 发送错误时，updateFaultItem中isolation为真，与latencyMax中值进行比较时得值为30s,也就时该broker在接下来得600000L，也就时5分钟内不提供服务，等待该Broker的恢复。
     * @param brokerName broker名称
     * @param currentLatency 本次消息发送延迟时间
     * @param isolation 是否隔离，该参数的含义如果为true,则使用默认时长30s来计算Broker故障规避时长，如果为false，则使用本次消息发送延迟时间来计算Broker故障规避时长
     */
    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
        if (this.sendLatencyFaultEnable) {
            long duration = computeNotAvailableDuration(isolation ? 30000 : currentLatency);
            this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration);
        }
    }

    private long computeNotAvailableDuration(final long currentLatency) {
        for (int i = latencyMax.length - 1; i >= 0; i--) {
            if (currentLatency >= latencyMax[i])
                return this.notAvailableDuration[i];
        }

        return 0;
    }
}
