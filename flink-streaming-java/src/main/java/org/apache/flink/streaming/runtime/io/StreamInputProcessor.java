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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer.DeserializationResult;
import org.apache.flink.runtime.io.network.api.serialization.SpillingAdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.NonReusingDeserializationDelegate;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StatusWatermarkValve;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Input reader for {@link org.apache.flink.streaming.runtime.tasks.OneInputStreamTask}.
 *
 * <p>This internally uses a {@link StatusWatermarkValve} to keep track of {@link Watermark} and
 * {@link StreamStatus} events, and forwards them to event subscribers once the
 * {@link StatusWatermarkValve} determines the {@link Watermark} from all inputs has advanced, or
 * that a {@link StreamStatus} needs to be propagated downstream to denote a status change.
 *
 * <p>Forwarding elements, watermarks, or status status elements must be protected by synchronizing
 * on the given lock object. This ensures that we don't call methods on a
 * {@link OneInputStreamOperator} concurrently with the timer callback or other things.
 *
 * @param <IN> The type of the record that can be read with this record reader.
 */
@Internal
public class StreamInputProcessor<IN> {

	private static final Logger LOG = LoggerFactory.getLogger(StreamInputProcessor.class);

	private final RecordDeserializer<DeserializationDelegate<StreamElement>>[] recordDeserializers;

	private RecordDeserializer<DeserializationDelegate<StreamElement>> currentRecordDeserializer;

	private final DeserializationDelegate<StreamElement> deserializationDelegate;

	private final CheckpointBarrierHandler barrierHandler;

	private final Object lock;

	// ---------------- Status and Watermark Valve ------------------

	/** Valve that controls how watermarks and stream statuses are forwarded. */
	private StatusWatermarkValve statusWatermarkValve;

	/** Number of input channels the valve needs to handle. */
	private final int numInputChannels;

	/**
	 * The channel from which a buffer came, tracked so that we can appropriately map
	 * the watermarks and watermark statuses to channel indexes of the valve.
	 */
	private int currentChannel = -1;

	private final StreamStatusMaintainer streamStatusMaintainer;

	private final OneInputStreamOperator<IN, ?> streamOperator;

	// ---------------- Metrics ------------------

	private final WatermarkGauge watermarkGauge;
	private Counter numRecordsIn;

	private boolean isFinished;

	@SuppressWarnings("unchecked")
	public StreamInputProcessor(
			InputGate[] inputGates,
			TypeSerializer<IN> inputSerializer,
			StreamTask<?, ?> checkpointedTask,
			CheckpointingMode checkpointMode,
			Object lock,
			IOManager ioManager,
			Configuration taskManagerConfig,
			StreamStatusMaintainer streamStatusMaintainer,
			OneInputStreamOperator<IN, ?> streamOperator,
			TaskIOMetricGroup metrics,
			WatermarkGauge watermarkGauge) throws IOException {

		InputGate inputGate = InputGateUtil.createInputGate(inputGates);

		this.barrierHandler = InputProcessorUtil.createCheckpointBarrierHandler(
			checkpointedTask, checkpointMode, ioManager, inputGate, taskManagerConfig);

		this.lock = checkNotNull(lock);

		StreamElementSerializer<IN> ser = new StreamElementSerializer<>(inputSerializer);
		this.deserializationDelegate = new NonReusingDeserializationDelegate<>(ser);

		// Initialize one deserializer per input channel
		this.recordDeserializers = new SpillingAdaptiveSpanningRecordDeserializer[inputGate.getNumberOfInputChannels()];

		for (int i = 0; i < recordDeserializers.length; i++) {
			recordDeserializers[i] = new SpillingAdaptiveSpanningRecordDeserializer<>(
				ioManager.getSpillingDirectoriesPaths());
		}

		this.numInputChannels = inputGate.getNumberOfInputChannels();

		this.streamStatusMaintainer = checkNotNull(streamStatusMaintainer);
		this.streamOperator = checkNotNull(streamOperator);

		this.statusWatermarkValve = new StatusWatermarkValve(
				numInputChannels,
				new ForwardingValveOutputHandler(streamOperator, lock));

		this.watermarkGauge = watermarkGauge;
		metrics.gauge("checkpointAlignmentTime", barrierHandler::getAlignmentDurationNanos);
	}

	/**
	 * 处理输入的数据，包括用户数据、watermark 和 checkpoint数据等
	 * 到这里整个flink数据流转的过程就结束了，简单总结一下：
	 * 1.启动一个环境
	 * 2.生成StreamGraph
	 * 3.注册和选举JobManager
	 * 4.在各节点生成TaskManager，并根据JobGraph生成对应的Task
	 * 5.启动各个task，准备执行代码
	 * @return
	 * @throws Exception
	 */
	public boolean processInput() throws Exception {
		if (isFinished) {
			return false;
		}
		if (numRecordsIn == null) {
			try {
				numRecordsIn = ((OperatorMetricGroup) streamOperator.getMetricGroup()).getIOMetricGroup().getNumRecordsInCounter();
			} catch (Exception e) {
				LOG.warn("An exception occurred during the metrics setup.", e);
				numRecordsIn = new SimpleCounter();
			}
		}

		//这个while是用来处理单个元素的（不要想当然以为是循环处理元素的）
		while (true) {
			//注意 1在下面
			// 2.接下来，会利用这个反序列化器得到下一个数据记录，并进行解析（是用户数据还是watermark等等），然后进行对应的操作
			if (currentRecordDeserializer != null) {
				DeserializationResult result = currentRecordDeserializer.getNextRecord(deserializationDelegate);

				if (result.isBufferConsumed()) {
					currentRecordDeserializer.getCurrentBuffer().recycleBuffer();
					currentRecordDeserializer = null;
				}

				if (result.isFullRecord()) {
					StreamElement recordOrMark = deserializationDelegate.getInstance();

					//如果元素是watermark，就准备更新当前channel的watermark 值（并不是简单赋值，因为有乱序存在），
					if (recordOrMark.isWatermark()) {
						// handle watermark
						statusWatermarkValve.inputWatermark(recordOrMark.asWatermark(), currentChannel);
						continue;

					//如果元素是status，就进行相应处理。可以看作是一个flag，标志着当前stream接下来即将没有元素输入（idle），
					// 或者当前即将由空闲状态转为有元素状态 （active）。同时，StreamStatus 还对如何处理watermark有影响。
					// 通过发送status，上游的operator可以很方便的通知下游当前的数据流的状态。
					} else if (recordOrMark.isStreamStatus()) {
						// handle stream status
						statusWatermarkValve.inputStreamStatus(recordOrMark.asStreamStatus(), currentChannel);
						continue;

					//LatencyMarker是用来衡量代码执行时间的。在Source处创建，携带创建时的时间戳，流到Sink时就可以知道经过了多长时间
					} else if (recordOrMark.isLatencyMarker()) {
						// handle latency marker
						synchronized (lock) {
							streamOperator.processLatencyMarker(recordOrMark.asLatencyMarker());
						}
						continue;
					//这里就是真正的，用户的代码即将被执行的地方。从章节1到这里足足用了三万字，有点万里长征的感觉
					} else {
						// now we can do the actual processing
						StreamRecord<IN> record = recordOrMark.asRecord();
						synchronized (lock) {
							numRecordsIn.inc();
							streamOperator.setKeyContextElement1(record);
							streamOperator.processElement(record);
						}
						return true;
					}
				}
			}
			//1.程序首先获取下一个buffer
			//这一段代码是服务于flink的FaultTorrent机制的，后面我会讲到，这里只需理解到它会尝试获取buffer，然后赋值给当前的反序列化器
			//每个元素都会触发这一段逻辑，如果下一个数据是buffer，则从外围的while循环里进入处理用户数据的逻辑；这个方法里默默的处理了barrier的逻辑
			// 处理barrier的过程在这段代码里没有体现，因为被包含在了 getNextNonBlocked() 方法中， 我们看下这个方法的核心逻辑：
			final BufferOrEvent bufferOrEvent = barrierHandler.getNextNonBlocked();
			if (bufferOrEvent != null) {
				if (bufferOrEvent.isBuffer()) {
					currentChannel = bufferOrEvent.getChannelIndex();
					currentRecordDeserializer = recordDeserializers[currentChannel];
					currentRecordDeserializer.setNextBuffer(bufferOrEvent.getBuffer());
				}
				else {
					// Event received
					final AbstractEvent event = bufferOrEvent.getEvent();
					if (event.getClass() != EndOfPartitionEvent.class) {
						throw new IOException("Unexpected event: " + event);
					}
				}
			}
			else {
				isFinished = true;
				if (!barrierHandler.isEmpty()) {
					throw new IllegalStateException("Trailing data in checkpoint barrier handler.");
				}
				return false;
			}
		}
	}

	public void cleanup() throws IOException {
		// clear the buffers first. this part should not ever fail
		for (RecordDeserializer<?> deserializer : recordDeserializers) {
			Buffer buffer = deserializer.getCurrentBuffer();
			if (buffer != null && !buffer.isRecycled()) {
				buffer.recycleBuffer();
			}
			deserializer.clear();
		}

		// cleanup the barrier handler resources
		barrierHandler.cleanup();
	}

	private class ForwardingValveOutputHandler implements StatusWatermarkValve.ValveOutputHandler {
		private final OneInputStreamOperator<IN, ?> operator;
		private final Object lock;

		private ForwardingValveOutputHandler(final OneInputStreamOperator<IN, ?> operator, final Object lock) {
			this.operator = checkNotNull(operator);
			this.lock = checkNotNull(lock);
		}

		@Override
		public void handleWatermark(Watermark watermark) {
			try {
				synchronized (lock) {
					watermarkGauge.setCurrentWatermark(watermark.getTimestamp());
					operator.processWatermark(watermark);
				}
			} catch (Exception e) {
				throw new RuntimeException("Exception occurred while processing valve output watermark: ", e);
			}
		}

		@SuppressWarnings("unchecked")
		@Override
		public void handleStreamStatus(StreamStatus streamStatus) {
			try {
				synchronized (lock) {
					streamStatusMaintainer.toggleStreamStatus(streamStatus);
				}
			} catch (Exception e) {
				throw new RuntimeException("Exception occurred while processing valve output stream status: ", e);
			}
		}
	}

}
