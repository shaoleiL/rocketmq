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
package org.apache.rocketmq.namesrv;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.Configuration;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.namesrv.kvconfig.KVConfigManager;
import org.apache.rocketmq.namesrv.processor.ClusterTestRequestProcessor;
import org.apache.rocketmq.namesrv.processor.DefaultRequestProcessor;
import org.apache.rocketmq.namesrv.routeinfo.BrokerHousekeepingService;
import org.apache.rocketmq.namesrv.routeinfo.RouteInfoManager;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.common.TlsMode;
import org.apache.rocketmq.remoting.netty.NettyRemotingServer;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;
import org.apache.rocketmq.srvutil.FileWatchService;

/**
 * NameServer的核心控制类
 *
 * 注：nettyServerConfig、remotingServer、remotingExecutor(下面的TODO1、2、3)
 * 这三个属性与网络通信有关，NameServer与Broker、Product、Consume之间的网络通信，基于Netty。
 */
public class NamesrvController {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

    // 主要指定nameserver的相关配置目录属性
    private final NamesrvConfig namesrvConfig;

    // TODO 1 与网络通信有关
    private final NettyServerConfig nettyServerConfig;

    /**
     * NameServer 定时任务执行线程池，一个线程，默认定时执行两个任务：
     * 任务1、每隔10s扫描broker,维护当前存活的Broker信息
     * 任务2、每隔10分钟打印KVConfig信息。
     */
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl(
        "NSScheduledThread"));
    //读取或变更NameServer的配置属性，加载NamesrvConfig中配置的配置文件到内存，
    // 此类一个亮点就是使用轻量级的非线程安全容器，再结合读写锁对资源读写进行保护。尽最大程度提高线程的并发度。
    private final KVConfigManager kvConfigManager;
    // NameServer数据的载体，记录Broker,Topic等信息
    private final RouteInfoManager routeInfoManager;
    // TODO 2 与网络通信有关
    private RemotingServer remotingServer;

    // BrokerHouseKeepingService实现 ChannelEventListener接口，可以说是通道在发送异常时的回调方法
    // （Nameserver与Broker的连接通道在关闭、通道发送异常、通道空闲时），在上述数据结构中移除已Down掉的Broker
    private BrokerHousekeepingService brokerHousekeepingService;
    // TODO 3 与网络通信有关
    private ExecutorService remotingExecutor;

    private Configuration configuration;
    private FileWatchService fileWatchService;

    public NamesrvController(NamesrvConfig namesrvConfig, NettyServerConfig nettyServerConfig) {
        // namesrv参数配置
        this.namesrvConfig = namesrvConfig;
        // netty的参数配置
        this.nettyServerConfig = nettyServerConfig;
        // kvConfigManager绑定NamesrvController
        this.kvConfigManager = new KVConfigManager(this);
        // 初始化RouteInfoManager，很重要，
        // 它负责缓存整个集群的broker信息，以及topic和queue的配置信息
        this.routeInfoManager = new RouteInfoManager();
        // 监听客户端连接(Channel)的变化，通知RouteInfoManager检查broker是否有变化
        this.brokerHousekeepingService = new BrokerHousekeepingService(this);
        this.configuration = new Configuration(log,this.namesrvConfig, this.nettyServerConfig);
        // namesrv的配置参数会保存到磁盘文件中
        this.configuration.setStorePathFromConfig(this.namesrvConfig, "configStorePath");
    }

    public boolean initialize() {
        // 加载KV配置表
        this.kvConfigManager.load();
        //  构造NettyRemotingServer，初始化通信层
        this.remotingServer = new NettyRemotingServer(this.nettyServerConfig, this.brokerHousekeepingService);

        // 初始化客户端请求处理的线程池
        // 创建一个线程容量为serverWorkerThreads的固定长度的线程池，默认为8，该线程池供DefaultRequestProcessor类实现，该类实现具体的默认的请求命令处理
        this.remotingExecutor = Executors.newFixedThreadPool(nettyServerConfig.getServerWorkerThreads(), new ThreadFactoryImpl("RemotingExecutorThread_"));  // @1

        // 注册DefaultRequestProcessor，所有的客户端请求都会转给这个Processor来处理
        // 它的逻辑到时候会出现在NettyServerHandler里
        // 注册默认请求处理器，用于每个请求代码在处理器表中没有完全匹配的情况，处理请求时使用的线程池即为remotingExecutor
        // 就是将DefaultRequestProcessor与代码@1创建的线程池绑定在一起
        this.registerProcessor();

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                // 启动定时调度，每10秒钟扫描所有Broker，检查存活状态
                NamesrvController.this.routeInfoManager.scanNotActiveBroker();
            }
        }, 5, 10, TimeUnit.SECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                // 每隔10分钟打印KV配置表的内容
                NamesrvController.this.kvConfigManager.printAllPeriodically();
            }
        }, 1, 10, TimeUnit.MINUTES);

        // 监听ssl证书文件变化
        if (TlsSystemConfig.tlsMode != TlsMode.DISABLED) {
            // Register a listener to reload SslContext
            // 注册一个监听器以重新加载SslContext
            try {
                fileWatchService = new FileWatchService(
                    new String[] {
                        TlsSystemConfig.tlsServerCertPath,
                        TlsSystemConfig.tlsServerKeyPath,
                        TlsSystemConfig.tlsServerTrustCertPath
                    },
                    new FileWatchService.Listener() {
                        boolean certChanged, keyChanged = false;
                        @Override
                        public void onChanged(String path) {
                            if (path.equals(TlsSystemConfig.tlsServerTrustCertPath)) {
                                log.info("The trust certificate changed, reload the ssl context");
                                reloadServerSslContext();
                            }
                            if (path.equals(TlsSystemConfig.tlsServerCertPath)) {
                                certChanged = true;
                            }
                            if (path.equals(TlsSystemConfig.tlsServerKeyPath)) {
                                keyChanged = true;
                            }
                            if (certChanged && keyChanged) {
                                log.info("The certificate and private key changed, reload the ssl context");
                                certChanged = keyChanged = false;
                                reloadServerSslContext();
                            }
                        }
                        private void reloadServerSslContext() {
                            ((NettyRemotingServer) remotingServer).loadSslContext();
                        }
                    });
            } catch (Exception e) {
                log.warn("FileWatchService created error, can't load the certificate dynamically");
            }
        }

        return true;
    }

    /**
     * 注册默认请求处理器，用于每个请求代码在处理器表中没有完全匹配的情况，处理请求时使用的线程池即为remotingExecutor
     */
    private void registerProcessor() {
        if (namesrvConfig.isClusterTest()) {

            this.remotingServer.registerDefaultProcessor(new ClusterTestRequestProcessor(this, namesrvConfig.getProductEnvName()),
                this.remotingExecutor);
        } else {

            this.remotingServer.registerDefaultProcessor(new DefaultRequestProcessor(this), this.remotingExecutor);
        }
    }

    public void start() throws Exception {
        // 启动netty server
        this.remotingServer.start();

        // 监听ssl文件变化，可以实时更新证书
        if (this.fileWatchService != null) {
            // 启动SslContext监听器线程
            this.fileWatchService.start();
        }
    }

    public void shutdown() {
        // 停止NettyRemotingServer
        this.remotingServer.shutdown();
        // 停止remoting线程池
        this.remotingExecutor.shutdown();
        // 停止定时任务线程池
        this.scheduledExecutorService.shutdown();

        if (this.fileWatchService != null) {
            // 停止SslContext监听器
            this.fileWatchService.shutdown();
        }
    }

    public NamesrvConfig getNamesrvConfig() {
        return namesrvConfig;
    }

    public NettyServerConfig getNettyServerConfig() {
        return nettyServerConfig;
    }

    public KVConfigManager getKvConfigManager() {
        return kvConfigManager;
    }

    public RouteInfoManager getRouteInfoManager() {
        return routeInfoManager;
    }

    public RemotingServer getRemotingServer() {
        return remotingServer;
    }

    public void setRemotingServer(RemotingServer remotingServer) {
        this.remotingServer = remotingServer;
    }

    public Configuration getConfiguration() {
        return configuration;
    }
}
