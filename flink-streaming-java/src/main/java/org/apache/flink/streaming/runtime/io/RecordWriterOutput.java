/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusProvider;
import org.apache.flink.streaming.runtime.tasks.OperatorChain;
import org.apache.flink.util.OutputTag;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Implementation of {@link Output} that sends data using a {@link RecordWriter}.
 *
 * 对于位于 OperatorChain 末尾的算子，它处理过的记录需要被其它 Task 消费，因此它的记录需要被写入 ResultPartition 。因此，Flink 提供了 RecordWriterOutput，它也实现了 WatermarkGaugeExposingOutput，
 * 但是它是通过 RecordWriter 输出接收到的消息记录。RecordWriter 是 ResultPartitionWriter 的一层包装，提供了将记录序列化到 buffer 中的功能。
 */
@Internal
public class RecordWriterOutput<OUT> implements OperatorChain.WatermarkGaugeExposingOutput<StreamRecord<OUT>> {

	private StreamRecordWriter<SerializationDelegate<StreamElement>> recordWriter;   //就是通过它来往下游发送数据的，详细的数据写出流程可以看 RecordWriter 类的代码解析

	private SerializationDelegate<StreamElement> serializationDelegate;

	private final StreamStatusProvider streamStatusProvider;

	private final OutputTag outputTag;

	private final WatermarkGauge watermarkGauge = new WatermarkGauge();

	@SuppressWarnings("unchecked")
	public RecordWriterOutput(
			StreamRecordWriter<SerializationDelegate<StreamRecord<OUT>>> recordWriter,
			TypeSerializer<OUT> outSerializer,
			OutputTag outputTag,
			StreamStatusProvider streamStatusProvider) {

		checkNotNull(recordWriter);
		this.outputTag = outputTag;
		// generic hack: cast the writer to generic Object type so we can use it
		// with multiplexed records and watermarks
		this.recordWriter = (StreamRecordWriter<SerializationDelegate<StreamElement>>)
				(StreamRecordWriter<?>) recordWriter;

		TypeSerializer<StreamElement> outRecordSerializer =
				new StreamElementSerializer<>(outSerializer);

		if (outSerializer != null) {
			serializationDelegate = new SerializationDelegate<StreamElement>(outRecordSerializer);
		}

		this.streamStatusProvider = checkNotNull(streamStatusProvider);
	}

	//这里， collect 方法
	@Override
	public void collect(StreamRecord<OUT> record) {
		if (this.outputTag != null) {
			// we are only responsible for emitting to the main input
			return;
		}

		pushToRecordWriter(record);
	}

	@Override
	public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
		if (this.outputTag == null || !this.outputTag.equals(outputTag)) {
			// we are only responsible for emitting to the side-output specified by our
			// OutputTag.
			return;
		}

		pushToRecordWriter(record);
	}

	//核心方法
	private <X> void pushToRecordWriter(StreamRecord<X> record) {
		serializationDelegate.setInstance(record);

		try {
			//这里已经对应上了数据输出的代码解析，可以跳到 RecordWriter类的emit() 方法，继续查看后续流程
			recordWriter.emit(serializationDelegate); // <<<<<< 核心，直接看这里， 调用emit()方法来往ResultPartition中写出数据，
		}
		catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}

	@Override
	public void emitWatermark(Watermark mark) {
		watermarkGauge.setCurrentWatermark(mark.getTimestamp());
		serializationDelegate.setInstance(mark);

		if (streamStatusProvider.getStreamStatus().isActive()) {
			try {
				recordWriter.broadcastEmit(serializationDelegate);
			} catch (Exception e) {
				throw new RuntimeException(e.getMessage(), e);
			}
		}
	}

	public void emitStreamStatus(StreamStatus streamStatus) {
		serializationDelegate.setInstance(streamStatus);

		try {
			recordWriter.broadcastEmit(serializationDelegate);
		}
		catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}

	@Override
	public void emitLatencyMarker(LatencyMarker latencyMarker) {
		serializationDelegate.setInstance(latencyMarker);

		try {
			recordWriter.randomEmit(serializationDelegate);
		}
		catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}

	public void broadcastEvent(AbstractEvent event) throws IOException {
		recordWriter.broadcastEvent(event);
	}

	public void flush() throws IOException {
		recordWriter.flushAll();
	}

	@Override
	public void close() {
		recordWriter.close();
	}

	@Override
	public Gauge<Long> getWatermarkGauge() {
		return watermarkGauge;
	}
}
