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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.NetworkSequenceViewReader;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.netty.NettyMessage.ErrorResponse;
import org.apache.flink.runtime.io.network.partition.ProducerFailedException;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;

import org.apache.flink.shaded.guava18.com.google.common.collect.Sets;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.apache.flink.runtime.io.network.netty.NettyMessage.BufferResponse;

/**
 * A nonEmptyReader of partition queues, which listens for channel writability changed
 * events before writing and flushing {@link Buffer} instances.
 *
 * NettyServer 的其中一个 ChannelHandler，
 * 包含了一个可以从中读取数据的 NetworkSequenceViewReader 队列
 * 它会监听 Netty Channel 的可写入状态，一旦可以写入数据，就会从 NetworkSequenceViewReader 消费数据写入 Netty Channel。
 *
 * 也就是负责将 ResultSubparition 中的数据通过网络发送给 RemoteInputChannel
 *
 * PartitionRequestQueue 会监听 Netty Channel 的可写入状态，当 Channel 可写入时，就会从 availableReaders 队列中取出 NetworkSequenceViewReader，读取数据并写入网络。
 * 可写入状态是 Netty 通过水位线进行控制的，NettyServer 在启动的时候会配置水位线，如果 Netty 输出缓冲中的字节数超过了高水位值，我们会等到其降到低水位值以下才继续写入数据。
 * 通过水位线机制确保不往网络中写入太多数据。
 */
class PartitionRequestQueue extends ChannelInboundHandlerAdapter {

	private static final Logger LOG = LoggerFactory.getLogger(PartitionRequestQueue.class);

	private final ChannelFutureListener writeListener = new WriteAndFlushNextMessageIfPossibleListener();

	/** The readers which are already enqueued available for transferring data. */
	//  >>> 重要，一个队列，队列中存放的是所有已经有数据的reader(reader本质就是对ResultSubPartitionView的一个封装，可以从中读取对应ResultSubPartition的输出数据)，
	//  reader也是一个listener，ResultSubPartition有数据写出时会收到通知
	private final ArrayDeque<NetworkSequenceViewReader> availableReaders = new ArrayDeque<>();

	/** All the readers created for the consumers' partition requests. */
	//每个下游的 input channel 对应一个reader(用来获取对应ResultSubPartition的输出数据)
	private final ConcurrentMap<InputChannelID, NetworkSequenceViewReader> allReaders = new ConcurrentHashMap<>();

	private final Set<InputChannelID> released = Sets.newHashSet();

	private boolean fatalError;

	private ChannelHandlerContext ctx;

	@Override
	public void channelRegistered(final ChannelHandlerContext ctx) throws Exception {
		if (this.ctx == null) {
			this.ctx = ctx;
		}

		super.channelRegistered(ctx);
	}

	/**
	 * 当CreditBasedSequenceNumberingViewReader(自身作为ResultSubPartitionView的监听器)收到它连接的 ResultSubPartitionView 发来的通知时(通知它有数据产生了)
	 * 回调这个方法，也就是通知当前这个channel handler，有一个reader中的ResultSubPartitionView开始产生数据了，
	 *
	 * @param reader  这个reader连接的 ResultSubPartitionView 开始产生数据了；
	 */
	void notifyReaderNonEmpty(final NetworkSequenceViewReader reader) {
		// The notification might come from the same thread. For the initial writes this
		// might happen before the reader has set its reference to the view, because
		// creating the queue and the initial notification happen in the same method call.
		// This can be resolved by separating the creation of the view and allowing
		// notifications.

		// TODO This could potentially have a bad performance impact as in the
		// worst case (network consumes faster than the producer) each buffer
		// will trigger a separate event loop task being scheduled.
		ctx.executor().execute(new Runnable() {   //触发一次用户自定义事件
			@Override
			public void run() {
				ctx.pipeline().fireUserEventTriggered(reader);
			}
		});
	}

	/**
	 * Try to enqueue the reader once receiving credit notification from the consumer or receiving
	 * non-empty reader notification from the producer.
	 *
	 * <p>NOTE: Only one thread would trigger the actual enqueue after checking the reader's
	 * availability, so there is no race condition here.
	 *
	 * 确保reader连接的 ResultSubPartitionView 中有数据可供消费，然后加入 availableReaders 这个队列中；
	 */
	private void enqueueAvailableReader(final NetworkSequenceViewReader reader) throws Exception {
		if (reader.isRegisteredAsAvailable() || !reader.isAvailable()) {
			return; //不可用直接返回
		}
		// Queue an available reader for consumption. If the queue is empty,
		// we try trigger the actual write. Otherwise this will be handled by
		// the writeAndFlushNextMessageIfPossible calls.
		boolean triggerWrite = availableReaders.isEmpty();
		registerAvailableReader(reader);  //加入队列

		if (triggerWrite) {
			//如果这是队列中第一个元素，调用 writeAndFlushNextMessageIfPossible 发送数据
			writeAndFlushNextMessageIfPossible(ctx.channel());
		}
	}

	/**
	 * Accesses internal state to verify reader registration in the unit tests.
	 *
	 * <p><strong>Do not use anywhere else!</strong>
	 *
	 * @return readers which are enqueued available for transferring data
	 */
	@VisibleForTesting
	ArrayDeque<NetworkSequenceViewReader> getAvailableReaders() {
		return availableReaders;
	}

	//由上一个channel handler发来通知，表示reader已经创建了，加入映射关系
	public void notifyReaderCreated(final NetworkSequenceViewReader reader) {
		allReaders.put(reader.getReceiverId(), reader);
	}

	public void cancel(InputChannelID receiverId) {
		ctx.pipeline().fireUserEventTriggered(receiverId);
	}

	public void close() {
		if (ctx != null) {
			ctx.channel().close();
		}
	}

	/**
	 * Adds unannounced credits from the consumer and enqueues the corresponding reader for this
	 * consumer (if not enqueued yet).
	 *
	 * @param receiverId The input channel id to identify the consumer.
	 * @param credit The unannounced credits of the consumer.
	 */
	//收到消费者发来的信用时，回调这个方法；
	void addCredit(InputChannelID receiverId, int credit) throws Exception {
		if (fatalError) {
			return;
		}

		NetworkSequenceViewReader reader = allReaders.get(receiverId);
		if (reader != null) {
			reader.addCredit(credit); //往reader中加信用

			enqueueAvailableReader(reader);
		} else {
			throw new IllegalStateException("No reader for receiverId = " + receiverId + " exists.");
		}
	}

	//自定义用户事件的处理
	@Override
	public void userEventTriggered(ChannelHandlerContext ctx, Object msg) throws Exception {
		// The user event triggered event loop callback is used for thread-safe
		// hand over of reader queues and cancelled producers.

		if (msg instanceof NetworkSequenceViewReader) {  //reader 连接着 ResultSubPartitionView，可以从中消费数据；
			enqueueAvailableReader((NetworkSequenceViewReader) msg); // 确保有数据，然后加入队列；
		} else if (msg.getClass() == InputChannelID.class) {
			// Release partition view that get a cancel request.
			InputChannelID toCancel = (InputChannelID) msg;
			if (released.contains(toCancel)) {
				return;
			}

			// Cancel the request for the input channel
			int size = availableReaders.size();
			for (int i = 0; i < size; i++) {
				NetworkSequenceViewReader reader = pollAvailableReader();
				if (reader.getReceiverId().equals(toCancel)) {
					reader.releaseAllResources();
					markAsReleased(reader.getReceiverId());
				} else {
					registerAvailableReader(reader);
				}
			}

			allReaders.remove(toCancel);
		} else {
			ctx.fireUserEventTriggered(msg);
		}
	}

	// >>>>> channel可写入时的回调方法，
	@Override
	public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
		//当前channel的读写状态发生变化
		writeAndFlushNextMessageIfPossible(ctx.channel());
	}

	/**
	 * 从 availableReaders 中消费数据，然后写入到channel中
	 */
	private void writeAndFlushNextMessageIfPossible(final Channel channel) throws IOException {
		if (fatalError || !channel.isWritable()) {
			//如果当前不可写入，则直接返回
			return;
		}

		// The logic here is very similar to the combined input gate and local
		// input channel logic. You can think of this class acting as the input
		// gate and the consumed views as the local input channels.

		BufferAndAvailability next = null;
		try {
			while (true) {
				//取出一个 reader
				NetworkSequenceViewReader reader = pollAvailableReader();

				// No queue with available data. We allow this here, because
				// of the write callbacks that are executed after each write.
				if (reader == null) {
					return;
				}

				next = reader.getNextBuffer();
				if (next == null) {
					//没有读到数据
					if (!reader.isReleased()) {
						//还没有释放当前 subpartition，继续处理下一个 reader
						continue;
					}
					markAsReleased(reader.getReceiverId());

					//出错了
					Throwable cause = reader.getFailureCause();
					if (cause != null) {
						ErrorResponse msg = new ErrorResponse(
							new ProducerFailedException(cause),
							reader.getReceiverId());

						ctx.writeAndFlush(msg);
					}
				} else {
					// 读到了数据
					// This channel was now removed from the available reader queue.
					// We re-add it into the queue if it is still available
					if (next.moreAvailable()) {
						//这个 reader 还可以读到更多的数据，继续加入队列
						registerAvailableReader(reader);
					}

					BufferResponse msg = new BufferResponse(
						next.buffer(),
						reader.getSequenceNumber(),
						reader.getReceiverId(),
						next.buffersInBacklog());

					if (isEndOfPartitionEvent(next.buffer())) {
						reader.notifySubpartitionConsumed();
						reader.releaseAllResources();

						markAsReleased(reader.getReceiverId());
					}

					// Write and flush and wait until this is done before
					// trying to continue with the next buffer.
					// 向 client 发送数据，发送成功之后通过 writeListener 的回调触发下一次发送
					channel.writeAndFlush(msg).addListener(writeListener);  //这个监听器挺有意思的，成功了就继续调当前这个方法，相当于一个递归

					return;
				}
			}
		} catch (Throwable t) {
			if (next != null) {
				next.buffer().recycleBuffer();
			}

			throw new IOException(t.getMessage(), t);
		}
	}

	//把一个有数据的reader放入 availableReaders 队列中；
	private void registerAvailableReader(NetworkSequenceViewReader reader) {
		availableReaders.add(reader);
		reader.setRegisteredAsAvailable(true);
	}

	//从队列中取出一个reader
	@Nullable
	private NetworkSequenceViewReader pollAvailableReader() {
		NetworkSequenceViewReader reader = availableReaders.poll();
		if (reader != null) {
			reader.setRegisteredAsAvailable(false);
		}
		return reader;
	}

	private boolean isEndOfPartitionEvent(Buffer buffer) throws IOException {
		return EventSerializer.isEvent(buffer, EndOfPartitionEvent.class);
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		releaseAllResources();

		ctx.fireChannelInactive();
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		handleException(ctx.channel(), cause);
	}

	private void handleException(Channel channel, Throwable cause) throws IOException {
		LOG.error("Encountered error while consuming partitions", cause);

		fatalError = true;
		releaseAllResources();

		if (channel.isActive()) {
			channel.writeAndFlush(new ErrorResponse(cause)).addListener(ChannelFutureListener.CLOSE);
		}
	}

	private void releaseAllResources() throws IOException {
		// note: this is only ever executed by one thread: the Netty IO thread!
		for (NetworkSequenceViewReader reader : allReaders.values()) {
			reader.releaseAllResources();
			markAsReleased(reader.getReceiverId());
		}

		availableReaders.clear();
		allReaders.clear();
	}

	/**
	 * Marks a receiver as released.
	 */
	private void markAsReleased(InputChannelID receiverId) {
		released.add(receiverId);
	}

	// This listener is called after an element of the current nonEmptyReader has been
	// flushed. If successful, the listener triggers further processing of the
	// queues.
	//发送数据的监听器
	private class WriteAndFlushNextMessageIfPossibleListener implements ChannelFutureListener {

		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			try {
				if (future.isSuccess()) {
					//发送成功，再次尝试写入
					writeAndFlushNextMessageIfPossible(future.channel());
				} else if (future.cause() != null) {
					handleException(future.channel(), future.cause());
				} else {
					handleException(future.channel(), new IllegalStateException("Sending cancelled by user."));
				}
			} catch (Throwable t) {
				handleException(future.channel(), t);
			}
		}
	}
}
