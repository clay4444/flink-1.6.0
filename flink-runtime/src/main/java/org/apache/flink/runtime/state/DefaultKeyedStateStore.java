/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.FoldingState;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.util.Preconditions;

import static java.util.Objects.requireNonNull;

/**
 * Default implementation of KeyedStateStore that currently forwards state registration to a {@link RuntimeContext}.
 *
 * KeyedStateStore定义了用于创建和管理托管keyed state的方法，分别对应ValueState,ListState，ReducingState以及AggregatingState以及MapState。相比于operator state,Keyed state的管理要更复杂一些
 * KeyedStateStore接口的具体实现是 DefaultKeyedStateStore,DefaultKeyedStateStore 拥有 KeyedStateBackend 的引用，所有的状态获取的方法实际上都由 KeyedStateBackend 来完成。
 */
public class DefaultKeyedStateStore implements KeyedStateStore {  //桥梁  KeyedStateStore 接口

	protected final KeyedStateBackend<?> keyedStateBackend; // <<<<<<< 这里，所有的状态获取都是通过具体的 KeyedStateBackend 来完成的
	protected final ExecutionConfig executionConfig;

	public DefaultKeyedStateStore(KeyedStateBackend<?> keyedStateBackend, ExecutionConfig executionConfig) {
		this.keyedStateBackend = Preconditions.checkNotNull(keyedStateBackend);
		this.executionConfig = Preconditions.checkNotNull(executionConfig);
	}


	//获取 ValueState
	@Override
	public <T> ValueState<T> getState(ValueStateDescriptor<T> stateProperties) {
		requireNonNull(stateProperties, "The state properties must not be null");
		try {
			stateProperties.initializeSerializerUnlessSet(executionConfig);
			return getPartitionedState(stateProperties);
		} catch (Exception e) {
			throw new RuntimeException("Error while getting state", e);
		}
	}

	//获取ListState
	@Override
	public <T> ListState<T> getListState(ListStateDescriptor<T> stateProperties) {
		requireNonNull(stateProperties, "The state properties must not be null");
		try {
			stateProperties.initializeSerializerUnlessSet(executionConfig);
			ListState<T> originalState = getPartitionedState(stateProperties);
			return new UserFacingListState<>(originalState);
		} catch (Exception e) {
			throw new RuntimeException("Error while getting state", e);
		}
	}

	//获取ReducingState
	@Override
	public <T> ReducingState<T> getReducingState(ReducingStateDescriptor<T> stateProperties) {
		requireNonNull(stateProperties, "The state properties must not be null");
		try {
			stateProperties.initializeSerializerUnlessSet(executionConfig);
			return getPartitionedState(stateProperties);
		} catch (Exception e) {
			throw new RuntimeException("Error while getting state", e);
		}
	}

	@Override
	public <IN, ACC, OUT> AggregatingState<IN, OUT> getAggregatingState(AggregatingStateDescriptor<IN, ACC, OUT> stateProperties) {
		requireNonNull(stateProperties, "The state properties must not be null");
		try {
			stateProperties.initializeSerializerUnlessSet(executionConfig);
			return getPartitionedState(stateProperties);
		} catch (Exception e) {
			throw new RuntimeException("Error while getting state", e);
		}
	}

	@Override
	public <T, ACC> FoldingState<T, ACC> getFoldingState(FoldingStateDescriptor<T, ACC> stateProperties) {
		requireNonNull(stateProperties, "The state properties must not be null");
		try {
			stateProperties.initializeSerializerUnlessSet(executionConfig);
			return getPartitionedState(stateProperties);
		} catch (Exception e) {
			throw new RuntimeException("Error while getting state", e);
		}
	}

	@Override
	public <UK, UV> MapState<UK, UV> getMapState(MapStateDescriptor<UK, UV> stateProperties) {
		requireNonNull(stateProperties, "The state properties must not be null");
		try {
			stateProperties.initializeSerializerUnlessSet(executionConfig);
			MapState<UK, UV> originalState = getPartitionedState(stateProperties);
			return new UserFacingMapState<>(originalState);
		} catch (Exception e) {
			throw new RuntimeException("Error while getting state", e);
		}
	}

	/**
	 * >>>>>>>> 最核心的方法，所有的state的获取都是通过这个方法；
	 */
	protected  <S extends State> S getPartitionedState(StateDescriptor<S, ?> stateDescriptor) throws Exception {
		return keyedStateBackend.getPartitionedState(
				VoidNamespace.INSTANCE,
				VoidNamespaceSerializer.INSTANCE,
				stateDescriptor);
	}
}
