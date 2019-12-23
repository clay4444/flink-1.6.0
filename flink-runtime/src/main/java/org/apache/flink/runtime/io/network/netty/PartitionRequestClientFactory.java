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
import org.apache.flink.runtime.io.network.NetworkClientHandler;
import org.apache.flink.runtime.io.network.netty.exception.LocalTransportException;
import org.apache.flink.runtime.io.network.netty.exception.RemoteTransportException;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;

import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Factory for {@link PartitionRequestClient} instances.
 *
 * <p>Instances of partition requests clients are shared among several {@link RemoteInputChannel}
 * instances.
 *
 * 构建 "inputChannel请求远程tm的ResultSubPartition的客户端"  的工厂类；
 */
class PartitionRequestClientFactory {

	private final NettyClient nettyClient;

	//key是 ConnectionID(连接id，标识一个连接)，value是最终的 "inputChannel请求远程tm的ResultSubPartition的客户端"
	private final ConcurrentMap<ConnectionID, Object> clients = new ConcurrentHashMap<ConnectionID, Object>();

	PartitionRequestClientFactory(NettyClient nettyClient) {
		this.nettyClient = nettyClient;
	}

	/**
	 * 创建： 用来请求远程tm上的 ResultSubPartition 的客户端：  "PartitionRequestClient"
	 *
	 * Atomically establishes a TCP connection to the given remote address and
	 * creates a {@link PartitionRequestClient} instance for this connection.
	 */
	PartitionRequestClient createPartitionRequestClient(ConnectionID connectionId) throws IOException, InterruptedException {
		Object entry;
		PartitionRequestClient client = null;

		while (client == null) {
			entry = clients.get(connectionId);

			if (entry != null) {
				// Existing channel or connecting channel
				if (entry instanceof PartitionRequestClient) {
					client = (PartitionRequestClient) entry;
				}
				else {
					ConnectingChannel future = (ConnectingChannel) entry;
					client = future.waitForChannel();

					clients.replace(connectionId, future, client);
				}
			}
			else {
				// No channel yet. Create one, but watch out for a race.
				// We create a "connecting future" and atomically add it to the map.
				// Only the thread that really added it establishes the channel.
				// The others need to wait on that original establisher's future.

				//这个连接还没有channel，说明还没有和远程的 NettyServer 建立连接；此时需要用 nettyClient 去连接远程ip，然后阻塞的返回创建成功的channel；
				ConnectingChannel connectingChannel = new ConnectingChannel(connectionId, this);  //监听连接是否建立成功的监听器；
				Object old = clients.putIfAbsent(connectionId, connectingChannel);

				if (old == null) {
					nettyClient.connect(connectionId.getAddress()).addListener(connectingChannel); //连接远程server，并添加监听器；

					client = connectingChannel.waitForChannel();  //阻塞的等待(通过加锁的方式来实现阻塞的)，从监听器中获取的最终的client，也就是说最终的client是在connectingChannel这个监听器中创建的；

					clients.replace(connectionId, connectingChannel, client);
				}
				else if (old instanceof ConnectingChannel) {
					client = ((ConnectingChannel) old).waitForChannel();

					clients.replace(connectionId, old, client);
				}
				else {
					client = (PartitionRequestClient) old;
				}
			}

			// Make sure to increment the reference count before handing a client
			// out to ensure correct bookkeeping for channel closing.
			if (!client.incrementReferenceCounter()) {
				destroyPartitionRequestClient(connectionId, client);
				client = null;
			}
		}

		return client;
	}

	public void closeOpenChannelConnections(ConnectionID connectionId) {
		Object entry = clients.get(connectionId);

		if (entry instanceof ConnectingChannel) {
			ConnectingChannel channel = (ConnectingChannel) entry;

			if (channel.dispose()) {
				clients.remove(connectionId, channel);
			}
		}
	}

	int getNumberOfActiveClients() {
		return clients.size();
	}

	/**
	 * Removes the client for the given {@link ConnectionID}.
	 */
	void destroyPartitionRequestClient(ConnectionID connectionId, PartitionRequestClient client) {
		clients.remove(connectionId, client);
	}

	/**
	 * 继承了ChannelFutureListener，所以当发生channel相关的事件时 (在这里是connect连接建立)，会触发回调
	 */
	private static final class ConnectingChannel implements ChannelFutureListener {

		private final Object connectLock = new Object();

		private final ConnectionID connectionId;

		private final PartitionRequestClientFactory clientFactory;

		private boolean disposeRequestClient = false;

		public ConnectingChannel(ConnectionID connectionId, PartitionRequestClientFactory clientFactory) {
			this.connectionId = connectionId;
			this.clientFactory = clientFactory;
		}

		private boolean dispose() {
			boolean result;
			synchronized (connectLock) {
				if (partitionRequestClient != null) {
					result = partitionRequestClient.disposeIfNotUsed();
				}
				else {
					disposeRequestClient = true;
					result = true;
				}

				connectLock.notifyAll();
			}

			return result;
		}

		//连接已成功建立进来这里； 创建 partitionRequestClient 的逻辑在这里；
		private void handInChannel(Channel channel) {  // channel 代表的是 已经建立的TCP连接
			synchronized (connectLock) {
				try {
					//这个 ChannelHandler 是干啥的？
					NetworkClientHandler clientHandler = channel.pipeline().get(NetworkClientHandler.class);
					partitionRequestClient = new PartitionRequestClient(   //>>>>>>>>>>> 最终创建 PartitionRequestClient，用来请求远程的 ResultSubPartition
						channel, clientHandler, connectionId, clientFactory);

					if (disposeRequestClient) {
						partitionRequestClient.disposeIfNotUsed();
					}

					connectLock.notifyAll();
				}
				catch (Throwable t) {
					notifyOfError(t);
				}
			}
		}

		private volatile PartitionRequestClient partitionRequestClient;

		private volatile Throwable error;

		//阻塞方式(通过锁)，等待channel建立完成，返回 partitionRequestClient(请求远程ResultSubPartition的客户端)
		//为什么需要加锁阻塞？因为上层NettyClient刚通过connect建立连接，就去获取了，此时连接还没建立呢，目的就是要保证channel已经建立成功了、partitionRequestClient已经创建了，再去获取
		private PartitionRequestClient waitForChannel() throws IOException, InterruptedException {
			synchronized (connectLock) {
				while (error == null && partitionRequestClient == null) {
					connectLock.wait(2000);
				}
			}

			if (error != null) {
				throw new IOException("Connecting the channel failed: " + error.getMessage(), error);
			}

			return partitionRequestClient;
		}

		private void notifyOfError(Throwable error) {
			synchronized (connectLock) {
				this.error = error;
				connectLock.notifyAll();
			}
		}

		//操作完成 (这里是connect连接建立完成)，回调这个方法；
		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			if (future.isSuccess()) {
				handInChannel(future.channel()); //连接成功建立；
			}
			else if (future.cause() != null) {
				notifyOfError(new RemoteTransportException(
						"Connecting to remote task manager + '" + connectionId.getAddress() +
								"' has failed. This might indicate that the remote task " +
								"manager has been lost.",
						connectionId.getAddress(), future.cause()));
			}
			else {
				notifyOfError(new LocalTransportException(
					String.format(
						"Connecting to remote task manager '%s' has been cancelled.",
						connectionId.getAddress()),
					null));
			}
		}
	}
}
