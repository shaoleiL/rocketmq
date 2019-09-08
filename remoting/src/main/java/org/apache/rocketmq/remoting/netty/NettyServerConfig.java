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
package org.apache.rocketmq.remoting.netty;

public class NettyServerConfig implements Cloneable {
    // NameServer监听端口，会被初始化为9876
    private int listenPort = 8888;

    // 含义：netty业务线程池的线程个数，RocketMQ按任务类型，每个任务类型会拥有一个专门的线程池，比如发送消息，消费消息，另外再加一个其他（默认的业务线程池），
    // 默认业务线程池，采用fixed类型，线程个数就是由serverWorkerThreads。
    // 线程名称：RemotingExecutorThread_
    // 作用范围：该参数目前主要用于NameServer的默认业务线程池，处理诸如broker,product,consume与NameServer的所有交互命令。
    // 源码来源：org.apache.rocketmq.namesrv.NamesrvController#initialize()
    private int serverWorkerThreads = 8;

    // 含义：业务线程池的线程个数，RocketMQ按任务类型，每个任务类型会拥有一个专门的线程池，比如发送消息，消费消息，另外再加一个其他（默认的业务线程池），
    // 默认业务线程池，采用fixed类型，线程个数就是由serverCallbackExecutorThreads 。
    // 线程名称：NettyServerPublicExecutor_
    // 作用范围：broker,product,consume处理默认命令的业务线程池大小。
    // 源码来源：org.apache.rocketmq.remoting.netty.NettyRemotingServer
    private int serverCallbackExecutorThreads = 0;

    // 含义：Netty IO线程数量，Selector所在的线程个数，也就主从Reactor模型中的从Reactor线程数量 。
    // 线程名称：NettyServerNIOSelector_
    // 作用范围：broker,product,consume 服务端的IO线程数量。
    // 源码来源：org.apache.rocketmq.remoting.netty.NettyRemotingServer
    private int serverSelectorThreads = 3;

    // 单向（Oneway）发送特点为只负责发送消息，不等待服务器回应且没有回调函数触发，即只发送请求不等待应答。此方式发送消息的过程耗时非常短，一般在微秒级别。
    // send oneway消息请求并发度（Broker端参数）
    private int serverOnewaySemaphoreValue = 256;
    // 异步消息发送最大并发度(Broker端参数)
    private int serverAsyncSemaphoreValue = 64;

    // 网络连接最大空闲时间，默认120S, 如果连接空闲时间超过该参数设置的值，连接将被关闭
    private int serverChannelMaxIdleTimeSeconds = 120;

    // socket发送缓存区大小，默认64K
    private int serverSocketSndBufSize = NettySystemConfig.socketSndbufSize;

    // socket接收缓存区大小，默认64K
    private int serverSocketRcvBufSize = NettySystemConfig.socketRcvbufSize;

    // ByteBuffer是否开启缓存，建议开启
    private boolean serverPooledByteBufAllocatorEnable = true;

    /**
     * make make install
     *
     *
     * ../glibc-2.10.1/configure \ --prefix=/usr \ --with-headers=/usr/include \
     * --host=x86_64-linux-gnu \ --build=x86_64-pc-linux-gnu \ --without-gd
     */
    // 是否启用Epoll IO模型，Linux环境建议开启
    private boolean useEpollNativeSelector = false;

    public int getListenPort() {
        return listenPort;
    }

    public void setListenPort(int listenPort) {
        this.listenPort = listenPort;
    }

    public int getServerWorkerThreads() {
        return serverWorkerThreads;
    }

    public void setServerWorkerThreads(int serverWorkerThreads) {
        this.serverWorkerThreads = serverWorkerThreads;
    }

    public int getServerSelectorThreads() {
        return serverSelectorThreads;
    }

    public void setServerSelectorThreads(int serverSelectorThreads) {
        this.serverSelectorThreads = serverSelectorThreads;
    }

    public int getServerOnewaySemaphoreValue() {
        return serverOnewaySemaphoreValue;
    }

    public void setServerOnewaySemaphoreValue(int serverOnewaySemaphoreValue) {
        this.serverOnewaySemaphoreValue = serverOnewaySemaphoreValue;
    }

    public int getServerCallbackExecutorThreads() {
        return serverCallbackExecutorThreads;
    }

    public void setServerCallbackExecutorThreads(int serverCallbackExecutorThreads) {
        this.serverCallbackExecutorThreads = serverCallbackExecutorThreads;
    }

    public int getServerAsyncSemaphoreValue() {
        return serverAsyncSemaphoreValue;
    }

    public void setServerAsyncSemaphoreValue(int serverAsyncSemaphoreValue) {
        this.serverAsyncSemaphoreValue = serverAsyncSemaphoreValue;
    }

    public int getServerChannelMaxIdleTimeSeconds() {
        return serverChannelMaxIdleTimeSeconds;
    }

    public void setServerChannelMaxIdleTimeSeconds(int serverChannelMaxIdleTimeSeconds) {
        this.serverChannelMaxIdleTimeSeconds = serverChannelMaxIdleTimeSeconds;
    }

    public int getServerSocketSndBufSize() {
        return serverSocketSndBufSize;
    }

    public void setServerSocketSndBufSize(int serverSocketSndBufSize) {
        this.serverSocketSndBufSize = serverSocketSndBufSize;
    }

    public int getServerSocketRcvBufSize() {
        return serverSocketRcvBufSize;
    }

    public void setServerSocketRcvBufSize(int serverSocketRcvBufSize) {
        this.serverSocketRcvBufSize = serverSocketRcvBufSize;
    }

    public boolean isServerPooledByteBufAllocatorEnable() {
        return serverPooledByteBufAllocatorEnable;
    }

    public void setServerPooledByteBufAllocatorEnable(boolean serverPooledByteBufAllocatorEnable) {
        this.serverPooledByteBufAllocatorEnable = serverPooledByteBufAllocatorEnable;
    }

    public boolean isUseEpollNativeSelector() {
        return useEpollNativeSelector;
    }

    public void setUseEpollNativeSelector(boolean useEpollNativeSelector) {
        this.useEpollNativeSelector = useEpollNativeSelector;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return (NettyServerConfig) super.clone();
    }
}
