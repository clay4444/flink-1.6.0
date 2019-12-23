/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;

import java.io.IOException;

/**
 * 在NetworkEnvironment中提供服务，所以也是每个tm一个？ 是的，
 * ConnectionManager的具体实现，用来管理所有的网络连接； Netty
 * 也就是说一个tm上既有Netty客户端也有Netty服务端；
 */
public class NettyConnectionManager implements ConnectionManager {

	//Netty服务端
	private final NettyServer server;

	//Netty客户端
	private final NettyClient client;

	private final NettyBufferPool bufferPool;

	//这个就是构建 "inputChannel请求远程tm的ResultSubPartition的客户端"  的工厂类；
	private final PartitionRequestClientFactory partitionRequestClientFactory;

	public NettyConnectionManager(NettyConfig nettyConfig) {
		this.server = new NettyServer(nettyConfig);
		this.client = new NettyClient(nettyConfig);
		this.bufferPool = new NettyBufferPool(nettyConfig.getNumberOfArenas());

		this.partitionRequestClientFactory = new PartitionRequestClientFactory(client); //把NettyClient传过了，所以肯定是通过NettyClient连接远程NettyServer 来实现的；
	}

	// ****** 注意：这个方法在tm刚启动的时候，就调用了，此时：NettyServer 和 NettyClient 也就构建好了；
	//NettyConnectionManager 在启动的时候会创建并启动 NettyClient 和 NettyServer，
	//NettyServer 会启动一个服务端监听，等待其它 NettyClient 的连接：
	@Override
	public void start(ResultPartitionProvider partitionProvider, TaskEventDispatcher taskEventDispatcher) throws IOException {
		NettyProtocol partitionRequestProtocol = new NettyProtocol(
			partitionProvider,
			taskEventDispatcher,
			client.getConfig().isCreditBasedEnabled());

		//初始化 Netty Client
		//和NettyServer不同的是，只是配置了引导了EventLoop，(还做了一些简单的配置)，并没有建立连接的步骤；
		//但是建立连接的过程还是在 NettyClient 中实现的(加 channel handler也在)， 只是在第一次请求远程 ResultSubPartition 的时候才被调用，
		client.init(partitionRequestProtocol, bufferPool);

		//初始化并启动 Netty Server
		//配置、启动引导，配置EventLoop、水位线、channelHandler， 而且这里已经开始在本地ip的某个端口上进行监听了；
		server.init(partitionRequestProtocol, bufferPool);
	}

	//创建 请求远程ResultSubPartition的客户端；
	//>>> 猜测：应该是某个InputChanel调用的；对，RemoteInputChannel#requestSubpartition 时调用的；
	@Override
	public PartitionRequestClient createPartitionRequestClient(ConnectionID connectionId)
			throws IOException, InterruptedException {
		//这里实际上会建立和其它 Task 的 Server 的连接
		//返回的 PartitionRequestClient 中封装了 netty channel 和 channel handler
		return partitionRequestClientFactory.createPartitionRequestClient(connectionId);
	}

	@Override
	public void closeOpenChannelConnections(ConnectionID connectionId) {
		partitionRequestClientFactory.closeOpenChannelConnections(connectionId);
	}

	@Override
	public int getNumberOfActiveConnections() {
		return partitionRequestClientFactory.getNumberOfActiveClients();
	}

	@Override
	public int getDataPort() {
		if (server != null && server.getLocalAddress() != null) {
			return server.getLocalAddress().getPort();
		} else {
			return -1;
		}
	}

	@Override
	public void shutdown() {
		client.shutdown();
		server.shutdown();
	}

	NettyClient getClient() {
		return client;
	}

	NettyServer getServer() {
		return server;
	}

	NettyBufferPool getBufferPool() {
		return bufferPool;
	}
}
