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
 * 在NetworkEnvironment中提供服务，所以也是每个tm一个？
 * ConnectionManager的具体实现，用来管理所有的网络连接； Netty
 */
public class NettyConnectionManager implements ConnectionManager {

	private final NettyServer server;

	private final NettyClient client;

	private final NettyBufferPool bufferPool;

	private final PartitionRequestClientFactory partitionRequestClientFactory;

	public NettyConnectionManager(NettyConfig nettyConfig) {
		this.server = new NettyServer(nettyConfig);
		this.client = new NettyClient(nettyConfig);
		this.bufferPool = new NettyBufferPool(nettyConfig.getNumberOfArenas());

		this.partitionRequestClientFactory = new PartitionRequestClientFactory(client);
	}

	//NettyConnectionManager 在启动的时候会创建并启动 NettyClient 和 NettyServer，NettyServer 会启动一个服务端监听，等待其它 NettyClient 的连接：
	@Override
	public void start(ResultPartitionProvider partitionProvider, TaskEventDispatcher taskEventDispatcher) throws IOException {
		NettyProtocol partitionRequestProtocol = new NettyProtocol(
			partitionProvider,
			taskEventDispatcher,
			client.getConfig().isCreditBasedEnabled());

		//初始化 Netty Client
		client.init(partitionRequestProtocol, bufferPool);

		//初始化并启动 Netty Server
		server.init(partitionRequestProtocol, bufferPool);
	}

	@Override
	public PartitionRequestClient createPartitionRequestClient(ConnectionID connectionId)
			throws IOException, InterruptedException {
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
