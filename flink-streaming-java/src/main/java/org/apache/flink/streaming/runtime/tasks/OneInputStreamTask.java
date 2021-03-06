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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.io.StreamInputProcessor;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;

import javax.annotation.Nullable;

/**
 * A {@link StreamTask} for executing a {@link OneInputStreamOperator}.
 *
 * 继承自 StreamTask(继承自AbstractInvokeable，任务执行时，就是执行的它的invoke方法)，
 * 它的主要执行逻辑就是不断循环调用 StreamInputProcessor.processInpt() 方法。
 *
 * StreamInputProcessor 从缓冲区中读取记录或 watermark 等消息，然后调用 streamOperator.processElement(record) 交给 head operator 进行处理，并依次将处理结果交给下游算子。
 */
@Internal
public class OneInputStreamTask<IN, OUT> extends StreamTask<OUT, OneInputStreamOperator<IN, OUT>> {

	private StreamInputProcessor<IN> inputProcessor;

	private volatile boolean running = true;

	private final WatermarkGauge inputWatermarkGauge = new WatermarkGauge();

	/**
	 * Constructor for initialization, possibly with initial state (recovery / savepoint / etc).
	 *
	 * @param env The task environment for this task.
	 */
	public OneInputStreamTask(Environment env) {
		super(env);
	}

	/**
	 * Constructor for initialization, possibly with initial state (recovery / savepoint / etc).
	 *
	 * <p>This constructor accepts a special {@link ProcessingTimeService}. By default (and if
	 * null is passes for the time provider) a {@link SystemProcessingTimeService DefaultTimerService}
	 * will be used.
	 *
	 * @param env The task environment for this task.
	 * @param timeProvider Optionally, a specific time provider to use.
	 */
	@VisibleForTesting
	public OneInputStreamTask(
			Environment env,
			@Nullable ProcessingTimeService timeProvider) {
		super(env, timeProvider);
	}

	/**
	 * 初始化 inputProcessor：inputProcessor就是处理用户数据，watermark，checkpoint的数据的;
	 * @throws Exception
	 */
	@Override
	public void init() throws Exception {

		//创建一个 StreamInputProcessor
		StreamConfig configuration = getConfiguration();

		TypeSerializer<IN> inSerializer = configuration.getTypeSerializerIn1(getUserCodeClassLoader());
		int numberOfInputs = configuration.getNumberOfInputs();

		if (numberOfInputs > 0) {
			InputGate[] inputGates = getEnvironment().getAllInputGates();

			// 生成 inputProcessor
			inputProcessor = new StreamInputProcessor<>(
					inputGates,
					inSerializer,
					this,
					configuration.getCheckpointMode(),
					getCheckpointLock(),
					getEnvironment().getIOManager(),
					getEnvironment().getTaskManagerInfo().getConfiguration(),
					getStreamStatusMaintainer(),
					this.headOperator,
					getEnvironment().getMetricGroup().getIOMetricGroup(),
					inputWatermarkGauge);
		}
		headOperator.getMetricGroup().gauge(MetricNames.IO_CURRENT_INPUT_WATERMARK, this.inputWatermarkGauge);
		// wrap watermark gauge since registered metrics must be unique
		getEnvironment().getMetricGroup().gauge(MetricNames.IO_CURRENT_INPUT_WATERMARK, this.inputWatermarkGauge::getValue);
	}

	/**
	 * Task对象在执行过程中，把执行的任务交给了StreamTask这个类去执行。在我们的 wordcount 例子中，
	 * 实际初始化的是OneInputStreamTask的对象（参考OneInputStreamTask的类继承图）。那么这个对象是如何执行用户的代码的呢？
	 *
	 * 这就是 run 方法所做的事；
	 *
	 * 它做的，就是把任务直接交给了InputProcessor去执行processInput方法。这是一个 StreamInputProcessor 的实例，
	 * 该processor的任务就是处理输入的数据，包括用户数据、watermark和checkpoint数据等
	 */
	@Override
	protected void run() throws Exception {
		// cache processor reference on the stack, to make the code more JIT friendly
		final StreamInputProcessor<IN> inputProcessor = this.inputProcessor;

		//循环调用 StreamInputProcessor.processInput 方法
		while (running && inputProcessor.processInput()) {
			// all the work happens in the "processInput" method
		}
	}

	@Override
	protected void cleanup() throws Exception {
		if (inputProcessor != null) {
			inputProcessor.cleanup();
		}
	}

	@Override
	protected void cancelTask() {
		running = false;
	}
}
