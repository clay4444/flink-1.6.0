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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.NetworkClientHandler;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.netty.exception.LocalTransportException;
import org.apache.flink.runtime.io.network.netty.exception.RemoteTransportException;
import org.apache.flink.runtime.io.network.netty.exception.TransportException;
import org.apache.flink.runtime.io.network.netty.NettyMessage.AddCredit;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.ArrayDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Channel handler to read the messages of buffer response or error response from the
 * producer, to write and flush the unannounced credits for the producer.
 *
 * <p>It is used in the new network credit-based mode.
 *
 * NettyClient 对应的channel handler，
 * 负责接收服务端通过 Netty channel 发送的数据，解析数据后交给对应的 RemoteInputChannle 进行处理：
 */
class CreditBasedPartitionRequestClientHandler extends ChannelInboundHandlerAdapter implements NetworkClientHandler {

	private static final Logger LOG = LoggerFactory.getLogger(CreditBasedPartitionRequestClientHandler.class);

	/** Channels, which already requested partitions from the producers. */
	//所有已经请求远端 ResultSubPartition 的 inputChannel
	private final ConcurrentMap<InputChannelID, RemoteInputChannel> inputChannels = new ConcurrentHashMap<>();

	/** Channels, which will notify the producers about unannounced credit. */
	private final ArrayDeque<RemoteInputChannel> inputChannelsWithCredit = new ArrayDeque<>();

	private final AtomicReference<Throwable> channelError = new AtomicReference<>();

	private final ChannelFutureListener writeListener = new WriteAndFlushNextMessageIfPossibleListener();

	/**
	 * Set of cancelled partition requests. A request is cancelled iff an input channel is cleared
	 * while data is still coming in for this channel.
	 */
	private final ConcurrentMap<InputChannelID, InputChannelID> cancelled = new ConcurrentHashMap<>();

	/**
	 * The channel handler context is initialized in channel active event by netty thread, the context may also
	 * be accessed by task thread or canceler thread to cancel partition request during releasing resources.
	 */
	private volatile ChannelHandlerContext ctx;

	// ------------------------------------------------------------------------
	// Input channel/receiver registration
	// ------------------------------------------------------------------------

	//PartitionRequestClient调用requestSubpartition请求远端subPartition之前，会调用这个方法，也就是告诉这个handler，这个 input channel 已经请求远端分区了，后续可能有它的返回数据
	@Override
	public void addInputChannel(RemoteInputChannel listener) throws IOException {
		checkError();

		if (!inputChannels.containsKey(listener.getInputChannelId())) {
			inputChannels.put(listener.getInputChannelId(), listener);
		}
	}

	@Override
	public void removeInputChannel(RemoteInputChannel listener) {
		inputChannels.remove(listener.getInputChannelId());
	}

	//取消对给定 receiverId 的订阅
	@Override
	public void cancelRequestFor(InputChannelID inputChannelId) {
		if (inputChannelId == null || ctx == null) {
			return;
		}

		if (cancelled.putIfAbsent(inputChannelId, inputChannelId) == null) {
			ctx.writeAndFlush(new NettyMessage.CancelPartitionRequest(inputChannelId)); //发送一个取消PartitionRequest的请求
		}
	}

	//通知生产端(NettyServer)，消费端已经申请了更多的buffer(信用)来接收上游数据
	@Override
	public void notifyCreditAvailable(final RemoteInputChannel inputChannel) {
		//有新的credit
		ctx.executor().execute(new Runnable() {
			@Override
			public void run() {
				ctx.pipeline().fireUserEventTriggered(inputChannel);
			}  //触发用户事件，跳到 userEventTriggered() 方法
		});
	}

	// ------------------------------------------------------------------------
	// Network events
	// ------------------------------------------------------------------------

	@Override
	public void channelActive(final ChannelHandlerContext ctx) throws Exception {
		if (this.ctx == null) {
			this.ctx = ctx;
		}

		super.channelActive(ctx);
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		// Unexpected close. In normal operation, the client closes the connection after all input
		// channels have been removed. This indicates a problem with the remote task manager.
		if (!inputChannels.isEmpty()) {
			final SocketAddress remoteAddr = ctx.channel().remoteAddress();

			notifyAllChannelsOfErrorAndClose(new RemoteTransportException(
				"Connection unexpectedly closed by remote task manager '" + remoteAddr + "'. "
					+ "This might indicate that the remote task manager was lost.", remoteAddr));
		}

		super.channelInactive(ctx);
	}

	/**
	 * Called on exceptions in the client handler pipeline.
	 *
	 * <p>Remote exceptions are received as regular payload.
	 */
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		if (cause instanceof TransportException) {
			notifyAllChannelsOfErrorAndClose(cause);
		} else {
			final SocketAddress remoteAddr = ctx.channel().remoteAddress();

			final TransportException tex;

			// Improve on the connection reset by peer error message
			if (cause instanceof IOException && cause.getMessage().equals("Connection reset by peer")) {
				tex = new RemoteTransportException("Lost connection to task manager '" + remoteAddr + "'. " +
					"This indicates that the remote task manager was lost.", remoteAddr, cause);
			} else {
				final SocketAddress localAddr = ctx.channel().localAddress();
				tex = new LocalTransportException(
					String.format("%s (connection to '%s')", cause.getMessage(), remoteAddr), localAddr, cause);
			}

			notifyAllChannelsOfErrorAndClose(tex);
		}
	}

	/**
	 * channel可读(收到了NettyServer返回的数据)时的回调方法
	 */
	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		//从netty channel中接收到数据
		try {
			//解析消息
			decodeMsg(msg);
		} catch (Throwable t) {
			notifyAllChannelsOfErrorAndClose(t);
		}
	}

	/**
	 * Triggered by notifying credit available in the client handler pipeline.
	 *
	 * <p>Enqueues the input channel and will trigger write&flush unannounced credits
	 * for this input channel if it is the first one in the queue.
	 */

	//触发用户事件，主要是给NettyServer发送一条消息(信用值增加了)
	@Override
	public void userEventTriggered(ChannelHandlerContext ctx, Object msg) throws Exception {
		if (msg instanceof RemoteInputChannel) {
			//有新的credit会触发
			boolean triggerWrite = inputChannelsWithCredit.isEmpty();

			//加入到队列中
			inputChannelsWithCredit.add((RemoteInputChannel) msg);

			if (triggerWrite) {
				writeAndFlushNextMessageIfPossible(ctx.channel()); //这里，给server发送消息
			}
		} else {
			ctx.fireUserEventTriggered(msg);
		}
	}

	@Override
	public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
		writeAndFlushNextMessageIfPossible(ctx.channel()); //这里
	}

	private void notifyAllChannelsOfErrorAndClose(Throwable cause) {
		if (channelError.compareAndSet(null, cause)) {
			try {
				for (RemoteInputChannel inputChannel : inputChannels.values()) {
					inputChannel.onError(cause);
				}
			} catch (Throwable t) {
				// We can only swallow the Exception at this point. :(
				LOG.warn("An Exception was thrown during error notification of a remote input channel.", t);
			} finally {
				inputChannels.clear();
				inputChannelsWithCredit.clear();

				if (ctx != null) {
					ctx.close();
				}
			}
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * Checks for an error and rethrows it if one was reported.
	 */
	private void checkError() throws IOException {
		final Throwable t = channelError.get();

		if (t != null) {
			if (t instanceof IOException) {
				throw (IOException) t;
			} else {
				throw new IOException("There has been an error in the channel.", t);
			}
		}
	}

	/**
	 * 解码从 NettyServer 返回的数据
	 */
	private void decodeMsg(Object msg) throws Throwable {
		final Class<?> msgClazz = msg.getClass();

		// ---- Buffer --------------------------------------------------------
		if (msgClazz == NettyMessage.BufferResponse.class) {
			//正常的数据
			NettyMessage.BufferResponse bufferOrEvent = (NettyMessage.BufferResponse) msg;

			//根据 ID 定位到当前数据是返回给哪个 RemoteInputChannel的 (也就是确定是哪个 RemoteInputChannel 请求的返回数据)
			RemoteInputChannel inputChannel = inputChannels.get(bufferOrEvent.receiverId);
			if (inputChannel == null) {
				//如果没有对应的 RemoteInputChannel
				bufferOrEvent.releaseBuffer();

				//取消对给定 receiverId 的订阅
				cancelRequestFor(bufferOrEvent.receiverId);

				return;
			}
			//解析消息，是buffer还是event
			decodeBufferOrEvent(inputChannel, bufferOrEvent);

		} else if (msgClazz == NettyMessage.ErrorResponse.class) {
			// ---- Error ---------------------------------------------------------
			NettyMessage.ErrorResponse error = (NettyMessage.ErrorResponse) msg;

			SocketAddress remoteAddr = ctx.channel().remoteAddress();

			if (error.isFatalError()) {
				notifyAllChannelsOfErrorAndClose(new RemoteTransportException(
					"Fatal error at remote task manager '" + remoteAddr + "'.",
					remoteAddr,
					error.cause));
			} else {
				RemoteInputChannel inputChannel = inputChannels.get(error.receiverId);

				if (inputChannel != null) {
					if (error.cause.getClass() == PartitionNotFoundException.class) {
						inputChannel.onFailedPartitionRequest();
					} else {
						inputChannel.onError(new RemoteTransportException(
							"Error at remote task manager '" + remoteAddr + "'.",
							remoteAddr,
							error.cause));
					}
				}
			}
		} else {
			throw new IllegalStateException("Received unknown message from producer: " + msg.getClass());
		}
	}

	/**
	 * 解析Netty返回的消息，是buffer还是event
	 */
	private void decodeBufferOrEvent(RemoteInputChannel inputChannel, NettyMessage.BufferResponse bufferOrEvent) throws Throwable {
		try {
			ByteBuf nettyBuffer = bufferOrEvent.getNettyBuffer();
			final int receivedSize = nettyBuffer.readableBytes();
			if (bufferOrEvent.isBuffer()) {
				// ---- Buffer ------------------------------------------------

				// Early return for empty buffers. Otherwise Netty's readBytes() throws an
				// IndexOutOfBoundsException.
				if (receivedSize == 0) {
					inputChannel.onEmptyBuffer(bufferOrEvent.sequenceNumber, bufferOrEvent.backlog);
					return;
				}

				//从对应的 RemoteInputChannel 中请求一个 Buffer
				Buffer buffer = inputChannel.requestBuffer();
				if (buffer != null) {
					//将接收的数据写入buffer
					nettyBuffer.readBytes(buffer.asByteBuf(), receivedSize);

					//通知对应的channel，backlog是生产者那边堆积的buffer数量
					inputChannel.onBuffer(buffer, bufferOrEvent.sequenceNumber, bufferOrEvent.backlog);
				} else if (inputChannel.isReleased()) {
					cancelRequestFor(bufferOrEvent.receiverId);
				} else {
					throw new IllegalStateException("No buffer available in credit-based input channel.");
				}
			} else {
				// ---- Event -------------------------------------------------
				// TODO We can just keep the serialized data in the Netty buffer and release it later at the reader
				byte[] byteArray = new byte[receivedSize];
				nettyBuffer.readBytes(byteArray);

				MemorySegment memSeg = MemorySegmentFactory.wrap(byteArray);

				//是一个事件，不需要从 RemoteInputChannel 中申请 buffer
				Buffer buffer = new NetworkBuffer(memSeg, FreeingBufferRecycler.INSTANCE, false);
				buffer.setSize(receivedSize);

				//通知对应的channel，backlog是生产者那边堆积的buffer数量
				inputChannel.onBuffer(buffer, bufferOrEvent.sequenceNumber, bufferOrEvent.backlog);
			}
		} finally {
			bufferOrEvent.releaseBuffer();
		}
	}

	/**
	 * Tries to write&flush unannounced credits for the next input channel in queue.
	 *
	 * <p>This method may be called by the first input channel enqueuing, or the complete
	 * future's callback in previous input channel, or the channel writability changed event.
	 */
	//给NettyServer发送一条消息(信用值增加了)，具体的发送动作
	private void writeAndFlushNextMessageIfPossible(Channel channel) {
		if (channelError.get() != null || !channel.isWritable()) {
			return;
		}

		//从队列中取出 RemoteInputChannel， 发送消息
		while (true) {
			RemoteInputChannel inputChannel = inputChannelsWithCredit.poll();  //拿出刚才添加的 inputChannel(带新的信用值的)

			// The input channel may be null because of the write callbacks
			// that are executed after each write.
			if (inputChannel == null) {
				return;
			}

			//It is no need to notify credit for the released channel.
			if (!inputChannel.isReleased()) {
				AddCredit msg = new AddCredit(  //这里，创建一条  AddCredit  消息
					inputChannel.getPartitionId(),
					inputChannel.getAndResetUnannouncedCredit(),  //获取并重置新增的credit
					inputChannel.getInputChannelId());

				// Write and flush and wait until this is done before
				// trying to continue with the next input channel.
				channel.writeAndFlush(msg).addListener(writeListener);  //这里，flush到NettyServer，

				return;
			}
		}
	}

	/**
	 *  发送完AddCredit消息的监听器，
	 */
	private class WriteAndFlushNextMessageIfPossibleListener implements ChannelFutureListener {

		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			try {
				if (future.isSuccess()) {
					writeAndFlushNextMessageIfPossible(future.channel());  //这个监听器和Server端内个类似，也是类似递归的调用
				} else if (future.cause() != null) {
					notifyAllChannelsOfErrorAndClose(future.cause());
				} else {
					notifyAllChannelsOfErrorAndClose(new IllegalStateException("Sending cancelled by user."));
				}
			} catch (Throwable t) {
				notifyAllChannelsOfErrorAndClose(t);
			}
		}
	}
}
