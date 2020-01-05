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
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;

import java.io.IOException;

/**
 * The CheckpointBarrierHandler reacts to checkpoint barrier arriving from the input channels.
 * Different implementations may either simply track barriers, or block certain inputs on
 * barriers.
 * 数据输入的逻辑在InputGate.java源码解析中；
 *
 *  之前我们说一个Task 通过循环调用 InputGate.getNextBufferOrEvent 方法获取输入数据，其实是不太准确的，其实是通过 StreamInputProcessor 创建的CheckpointBarrierHandler 处理的输入数据，
 *  CheckpointBarrierHandler是对InputGate的又一层封装，也就是 StreamInputProcessor -> CheckpointBarrierHandler -> InputGate -> InputChannel 四层，
 *
 *  CheckpointBarrierHandler有两种具体实现
 *            1.BarrierTracker 对应 AT_LEAST_ONCE    <<  具体看一下它的实现
 *            2.BarrierBuffer 对应 EXACTLY_ONCE
 */
@Internal
public interface CheckpointBarrierHandler {

	/**
	 * Returns the next {@link BufferOrEvent} that the operator may consume.
	 * This call blocks until the next BufferOrEvent is available, or until the stream
	 * has been determined to be finished.
	 *
	 * @return The next BufferOrEvent, or {@code null}, if the stream is finished.
	 *
	 * @throws IOException Thrown if the network or local disk I/O fails.
	 *
	 * @throws InterruptedException Thrown if the thread is interrupted while blocking during
	 *                              waiting for the next BufferOrEvent to become available.
	 * @throws Exception Thrown in case that a checkpoint fails that is started as the result of receiving
	 *                   the last checkpoint barrier
	 */
	BufferOrEvent getNextNonBlocked() throws Exception;

	/**
	 * Registers the task be notified once all checkpoint barriers have been received for a checkpoint.
	 *
	 * @param task The task to notify
	 */
	void registerCheckpointEventHandler(AbstractInvokable task);

	/**
	 * Cleans up all internally held resources.
	 *
	 * @throws IOException Thrown if the cleanup of I/O resources failed.
	 */
	void cleanup() throws IOException;

	/**
	 * Checks if the barrier handler has buffered any data internally.
	 * @return {@code True}, if no data is buffered internally, {@code false} otherwise.
	 */
	boolean isEmpty();

	/**
	 * Gets the time that the latest alignment took, in nanoseconds.
	 * If there is currently an alignment in progress, it will return the time spent in the
	 * current alignment so far.
	 *
	 * @return The duration in nanoseconds
	 */
	long getAlignmentDurationNanos();
}
