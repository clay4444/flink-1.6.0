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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.util.functions.StreamingFunctionUtils;

import static java.util.Objects.requireNonNull;

/**
 * This is used as the base class for operators that have a user-defined
 * function. This class handles the opening and closing of the user-defined functions,
 * as part of the operator life cycle.
 *
 * @param <OUT>
 *            The output type of the operator
 * @param <F>
 *            The type of the user function
 */

/**
 * flink为我们封装了一个算子的基类 AbstractUdfStreamOperator ，
 * 提供了一些通用功能，比如把context赋给算子，保存快照等等，其中最为大家了解的应该是这两 个：open 和close，此外还有snapshot，initState等
 * 它基本实现了这些；
 * 实现过程其实也就是使用 FunctionUtils.openFunction / closeFunction ，底层直接把用户的 userFunction 强转为 RichFunction 然后直接进行方法调用
 *
 * 至于具体到诸如 map/fliter 等操作对应的 StreamOperator，基本都是在 AbstractUdfStreamOperator 的基础上实现的。
 */
@PublicEvolving
public abstract class AbstractUdfStreamOperator<OUT, F extends Function>
		extends AbstractStreamOperator<OUT>
		implements OutputTypeConfigurable<OUT> {

	private static final long serialVersionUID = 1L;


	/** The user function. */
	protected final F userFunction;

	/** Flag to prevent duplicate function.close() calls in close() and dispose(). */
	private transient boolean functionsClosed = false;

	public AbstractUdfStreamOperator(F userFunction) {
		this.userFunction = requireNonNull(userFunction);
		checkUdfCheckpointingPreconditions();
	}

	/**
	 * Gets the user function executed in this operator.
	 * @return The user function of this operator.
	 */
	public F getUserFunction() {
		return userFunction;
	}

	// ------------------------------------------------------------------------
	//  operator life cycle
	// ------------------------------------------------------------------------

	@Override
	public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<OUT>> output) {
		super.setup(containingTask, config, output);
		FunctionUtils.setFunctionRuntimeContext(userFunction, getRuntimeContext());

	}

	@Override
	public void snapshotState(StateSnapshotContext context) throws Exception {
		super.snapshotState(context);
		StreamingFunctionUtils.snapshotFunctionState(context, getOperatorStateBackend(), userFunction);
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		super.initializeState(context);
		StreamingFunctionUtils.restoreFunctionState(context, userFunction);
	}

	@Override
	public void open() throws Exception {
		super.open();
		FunctionUtils.openFunction(userFunction, new Configuration());
	}

	@Override
	public void close() throws Exception {
		super.close();
		functionsClosed = true;
		FunctionUtils.closeFunction(userFunction);
	}

	@Override
	public void dispose() throws Exception {
		super.dispose();
		if (!functionsClosed) {
			functionsClosed = true;
			FunctionUtils.closeFunction(userFunction);
		}
	}

	// ------------------------------------------------------------------------
	//  checkpointing and recovery
	// ------------------------------------------------------------------------

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		super.notifyCheckpointComplete(checkpointId);

		if (userFunction instanceof CheckpointListener) {
			((CheckpointListener) userFunction).notifyCheckpointComplete(checkpointId);
		}
	}

	// ------------------------------------------------------------------------
	//  Output type configuration
	// ------------------------------------------------------------------------

	@Override
	public void setOutputType(TypeInformation<OUT> outTypeInfo, ExecutionConfig executionConfig) {
		StreamingFunctionUtils.setOutputType(userFunction, outTypeInfo, executionConfig);
	}


	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	/**
	 * Since the streaming API does not implement any parametrization of functions via a
	 * configuration, the config returned here is actually empty.
	 *
	 * @return The user function parameters (currently empty)
	 */
	public Configuration getUserFunctionParameters() {
		return new Configuration();
	}

	private void checkUdfCheckpointingPreconditions() {

		if (userFunction instanceof CheckpointedFunction
			&& userFunction instanceof ListCheckpointed) {

			throw new IllegalStateException("User functions are not allowed to implement " +
				"CheckpointedFunction AND ListCheckpointed.");
		}
	}
}
