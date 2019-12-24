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

import org.apache.flink.runtime.io.network.NetworkSequenceViewReader;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.netty.NettyMessage.AddCredit;
import org.apache.flink.runtime.io.network.netty.NettyMessage.CancelPartitionRequest;
import org.apache.flink.runtime.io.network.netty.NettyMessage.CloseRequest;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.runtime.io.network.netty.NettyMessage.PartitionRequest;
import static org.apache.flink.runtime.io.network.netty.NettyMessage.TaskEventRequest;

/**
 * Channel handler to initiate data transfers and dispatch backwards flowing task events.
 *
 * NettyServer 的其中一个 ChannelHandler，
 * 负责处理消费端通过 PartitionRequestClient 发送的 PartitionRequest 和 AddCredit 等请求
 */
class PartitionRequestServerHandler extends SimpleChannelInboundHandler<NettyMessage> {

	private static final Logger LOG = LoggerFactory.getLogger(PartitionRequestServerHandler.class);

	private final ResultPartitionProvider partitionProvider;

	private final TaskEventDispatcher taskEventDispatcher;

	//另外一个 Channel Handler
	private final PartitionRequestQueue outboundQueue;

	private final boolean creditBasedEnabled;

	PartitionRequestServerHandler(
		ResultPartitionProvider partitionProvider,
		TaskEventDispatcher taskEventDispatcher,
		PartitionRequestQueue outboundQueue,
		boolean creditBasedEnabled) {

		this.partitionProvider = partitionProvider;
		this.taskEventDispatcher = taskEventDispatcher;
		this.outboundQueue = outboundQueue;
		this.creditBasedEnabled = creditBasedEnabled;
	}

	@Override
	public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
		super.channelRegistered(ctx);
	}

	@Override
	public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
		super.channelUnregistered(ctx);
	}

	//channel可读的回调方法；
	@Override
	protected void channelRead0(ChannelHandlerContext ctx, NettyMessage msg) throws Exception {
		try {
			Class<?> msgClazz = msg.getClass();

			// ----------------------------------------------------------------
			// Intermediate result partition requests
			// ----------------------------------------------------------------
			if (msgClazz == PartitionRequest.class) {     //Server 端接收到 client 发送的 PartitionRequest, 接下来做了什么呢？
				PartitionRequest request = (PartitionRequest) msg;

				LOG.debug("Read channel on {}: {}.", ctx.channel().localAddress(), request);

				try {
					/**
					 * 1. 创建了一个 NetworkSequenceViewReader 对象，它相当于对 ResultSubpartitionView 的一层包装，
					 * 她会按顺序为读取的每一个 buffer 分配一个序列号，并且记录了接收数据的 RemoteInputChannel 的 ID。
					 * 在使用 Credit-based Flow Control 的情况下，NetworkSequenceViewReader 的具体实现对应为 CreditBasedSequenceNumberingViewReader。
					 * CreditBasedSequenceNumberingViewReader 同时还实现了 BufferAvailabilityListener 接口，因而可以作为 PipelinedSubpartitionView 的回调对象(也就是说当上游的ResultSubpartitionView有数据产生时，会通知这个类)
					 */
					NetworkSequenceViewReader reader;
					if (creditBasedEnabled) {
						reader = new CreditBasedSequenceNumberingViewReader(
							request.receiverId,
							request.credit,
							outboundQueue);
					} else {
						reader = new SequenceNumberingViewReader(
							request.receiverId,
							outboundQueue);
					}

					//2.请求创建 ResultSubpartitionView(这个是我们之前熟悉的，用来消费对应的ResultSubPartition的数据的)，看到底，最终还是调用的 ResultPartitionManager#createSubpartitionView
					reader.requestSubpartitionView(
						partitionProvider,
						request.partitionId,
						request.queueIndex);

					outboundQueue.notifyReaderCreated(reader); //3. 把创建的Reader放入了另外一个 channel handler
				} catch (PartitionNotFoundException notFound) {
					respondWithError(ctx, notFound, request.receiverId);
				}
			}
			// ----------------------------------------------------------------
			// Task events
			// ----------------------------------------------------------------
			else if (msgClazz == TaskEventRequest.class) {
				TaskEventRequest request = (TaskEventRequest) msg;

				if (!taskEventDispatcher.publish(request.partitionId, request.event)) {
					respondWithError(ctx, new IllegalArgumentException("Task event receiver not found."), request.receiverId);
				}
			} else if (msgClazz == CancelPartitionRequest.class) {
				CancelPartitionRequest request = (CancelPartitionRequest) msg;

				outboundQueue.cancel(request.receiverId);
			} else if (msgClazz == CloseRequest.class) {
				outboundQueue.close();
			} else if (msgClazz == AddCredit.class) {  //消费者可以消费更多数据了；
				//增加 credit
				AddCredit request = (AddCredit) msg;

				outboundQueue.addCredit(request.receiverId, request.credit);  //增加信用
			} else {
				LOG.warn("Received unexpected client request: {}", msg);
			}
		} catch (Throwable t) {
			respondWithError(ctx, t);
		}
	}

	private void respondWithError(ChannelHandlerContext ctx, Throwable error) {
		ctx.writeAndFlush(new NettyMessage.ErrorResponse(error));
	}

	private void respondWithError(ChannelHandlerContext ctx, Throwable error, InputChannelID sourceId) {
		LOG.debug("Responding with error: {}.", error.getClass());

		ctx.writeAndFlush(new NettyMessage.ErrorResponse(error, sourceId));
	}
}
