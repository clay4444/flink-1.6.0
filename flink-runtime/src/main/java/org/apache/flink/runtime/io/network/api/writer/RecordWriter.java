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

package org.apache.flink.runtime.io.network.api.writer;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordSerializer;
import org.apache.flink.runtime.io.network.api.serialization.SpanningRecordSerializer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.util.XORShiftRandom;

import java.io.IOException;
import java.util.Optional;
import java.util.Random;

import static org.apache.flink.runtime.io.network.api.serialization.RecordSerializer.SerializationResult;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A record-oriented runtime result writer.
 *
 * <p>The RecordWriter wraps the runtime's {@link ResultPartitionWriter} and takes care of
 * serializing records into buffers.
 *
 * <p><strong>Important</strong>: it is necessary to call {@link #flushAll()} after
 * all records have been written with {@link #emit(IOReadableWritable)}. This
 * ensures that all produced records are written to the output stream (incl.
 * partially filled ones).
 *
 * @param <T> the type of the record that can be emitted with this record writer
 */

/**
 * Task 通过 RecordWriter 将结果写入 ResultPartition 中。
 * RecordWriter 是对 ResultPartitionWriter 的一层封装，并负责将记录对象序列化到 buffer 中。先来看一下 RecordWriter 的成员变量和构造函数：
 *
 * 当 Task 通过 RecordWriter 输出一条记录时，主要流程为：
 * 1. 通过 ChannelSelector 确定写入的目标 channel
 * 2. 使用 RecordSerializer 对记录进行序列化
 * 3. 向 ResultPartition 请求 BufferBuilder，用于写入序列化结果
 * 4. 向 ResultPartition 添加 BufferConsumer，用于读取写入 Buffer 的数据
 */
public class RecordWriter<T extends IOReadableWritable> {

	//底层的 ResultPartition,  ResultPartition implements ResultPartitionWriter,  所以targetPartition 就代表要写入的ResultPartition；也即是Task的输出
	protected final ResultPartitionWriter targetPartition;

	//决定一条记录应该写入哪一个channel， 即 sub-partition，yes， sub-partition 和 下游的channel 一一对应；
	private final ChannelSelector<T> channelSelector;

	//channel的数量，即 sub-partition的数量
	private final int numChannels;

	/**
	 * {@link RecordSerializer} per outgoing channel.
	 */
	//序列化器
	private final RecordSerializer<T>[] serializers;

	//供每一个 channel 写入数据使用
	private final Optional<BufferBuilder>[] bufferBuilders;

	private final Random rng = new XORShiftRandom();

	private final boolean flushAlways;

	private Counter numBytesOut = new SimpleCounter();

	public RecordWriter(ResultPartitionWriter writer) {
		this(writer, new RoundRobinChannelSelector<T>());
	}

	@SuppressWarnings("unchecked")
	public RecordWriter(ResultPartitionWriter writer, ChannelSelector<T> channelSelector) {
		this(writer, channelSelector, false);
	}

	/**
	 * 构建RecordWriter；
	 * @param writer    指的就是这个Task要写入的ResultPartition
	 * @param channelSelector
	 * @param flushAlways
	 */
	public RecordWriter(ResultPartitionWriter writer, ChannelSelector<T> channelSelector, boolean flushAlways) {
		this.flushAlways = flushAlways;
		this.targetPartition = writer;
		this.channelSelector = channelSelector;

		this.numChannels = writer.getNumberOfSubpartitions();

		/*
		 * The runtime exposes a channel abstraction for the produced results
		 * (see {@link ChannelSelector}). Every channel has an independent
		 * serializer.
		 */
		//序列化器，用于将一条记录序列化到多个buffer中
		this.serializers = new SpanningRecordSerializer[numChannels];

		this.bufferBuilders = new Optional[numChannels];
		for (int i = 0; i < numChannels; i++) {
			serializers[i] = new SpanningRecordSerializer<T>();
			bufferBuilders[i] = Optional.empty();
		}
	}

	public void emit(T record) throws IOException, InterruptedException {
		for (int targetChannel : channelSelector.selectChannels(record, numChannels)) {
			sendToTarget(record, targetChannel);
		}
	}

	/**
	 * This is used to broadcast Streaming Watermarks in-band with records. This ignores
	 * the {@link ChannelSelector}.
	 */
	public void broadcastEmit(T record) throws IOException, InterruptedException {
		for (int targetChannel = 0; targetChannel < numChannels; targetChannel++) {
			sendToTarget(record, targetChannel);
		}
	}

	/**
	 * This is used to send LatencyMarks to a random target channel.
	 */
	public void randomEmit(T record) throws IOException, InterruptedException {
		sendToTarget(record, rng.nextInt(numChannels));
	}

	private void sendToTarget(T record, int targetChannel) throws IOException, InterruptedException {
		RecordSerializer<T> serializer = serializers[targetChannel];

		SerializationResult result = serializer.addRecord(record);

		while (result.isFullBuffer()) {
			if (tryFinishCurrentBufferBuilder(targetChannel, serializer)) {
				// If this was a full record, we are done. Not breaking
				// out of the loop at this point will lead to another
				// buffer request before breaking out (that would not be
				// a problem per se, but it can lead to stalls in the
				// pipeline).
				if (result.isFullRecord()) {
					break;
				}
			}
			BufferBuilder bufferBuilder = requestNewBufferBuilder(targetChannel);

			result = serializer.continueWritingWithNextBufferBuilder(bufferBuilder);
		}
		checkState(!serializer.hasSerializedData(), "All data should be written at once");

		if (flushAlways) {
			targetPartition.flush(targetChannel);
		}
	}

	public void broadcastEvent(AbstractEvent event) throws IOException {
		try (BufferConsumer eventBufferConsumer = EventSerializer.toBufferConsumer(event)) {
			for (int targetChannel = 0; targetChannel < numChannels; targetChannel++) {
				RecordSerializer<T> serializer = serializers[targetChannel];

				tryFinishCurrentBufferBuilder(targetChannel, serializer);

				// retain the buffer so that it can be recycled by each channel of targetPartition
				targetPartition.addBufferConsumer(eventBufferConsumer.copy(), targetChannel);
			}

			if (flushAlways) {
				flushAll();
			}
		}
	}

	public void flushAll() {
		targetPartition.flushAll();
	}

	public void clearBuffers() {
		for (int targetChannel = 0; targetChannel < numChannels; targetChannel++) {
			RecordSerializer<?> serializer = serializers[targetChannel];
			closeBufferBuilder(targetChannel);
			serializer.clear();
		}
	}

	/**
	 * Sets the metric group for this RecordWriter.
     */
	public void setMetricGroup(TaskIOMetricGroup metrics) {
		numBytesOut = metrics.getNumBytesOutCounter();
	}

	/**
	 * Marks the current {@link BufferBuilder} as finished and clears the state for next one.
	 *
	 * @return true if some data were written
	 */
	private boolean tryFinishCurrentBufferBuilder(int targetChannel, RecordSerializer<T> serializer) {

		if (!bufferBuilders[targetChannel].isPresent()) {
			return false;
		}
		BufferBuilder bufferBuilder = bufferBuilders[targetChannel].get();
		bufferBuilders[targetChannel] = Optional.empty();

		numBytesOut.inc(bufferBuilder.finish());
		serializer.clear();
		return true;
	}

	private BufferBuilder requestNewBufferBuilder(int targetChannel) throws IOException, InterruptedException {
		checkState(!bufferBuilders[targetChannel].isPresent());
		BufferBuilder bufferBuilder = targetPartition.getBufferProvider().requestBufferBuilderBlocking();
		bufferBuilders[targetChannel] = Optional.of(bufferBuilder);
		targetPartition.addBufferConsumer(bufferBuilder.createBufferConsumer(), targetChannel);
		return bufferBuilder;
	}

	private void closeBufferBuilder(int targetChannel) {
		if (bufferBuilders[targetChannel].isPresent()) {
			bufferBuilders[targetChannel].get().finish();
			bufferBuilders[targetChannel] = Optional.empty();
		}
	}
}
