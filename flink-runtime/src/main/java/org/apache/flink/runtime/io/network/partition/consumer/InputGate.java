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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.runtime.event.TaskEvent;

import java.io.IOException;
import java.util.Optional;

/**
 * An input gate consumes one or more partitions of a single produced intermediate result.
 *
 * <p> Each intermediate result is partitioned over its producing parallel subtasks; each of these
 * partitions is furthermore partitioned into one or more subpartitions.
 *
 * <p> As an example, consider a map-reduce program, where the map operator produces data and the
 * reduce operator consumes the produced data.
 *
 * <pre>{@code
 * +-----+              +---------------------+              +--------+
 * | Map | = produce => | Intermediate Result | <= consume = | Reduce |
 * +-----+              +---------------------+              +--------+
 * }</pre>
 *
 * <p> When deploying such a program in parallel, the intermediate result will be partitioned over its
 * producing parallel subtasks; each of these partitions is furthermore partitioned into one or more
 * subpartitions.
 *
 * <pre>{@code
 *                            Intermediate result
 *               +-----------------------------------------+
 *               |                      +----------------+ |              +-----------------------+
 * +-------+     | +-------------+  +=> | Subpartition 1 | | <=======+=== | Input Gate | Reduce 1 |
 * | Map 1 | ==> | | Partition 1 | =|   +----------------+ |         |    +-----------------------+
 * +-------+     | +-------------+  +=> | Subpartition 2 | | <==+    |
 *               |                      +----------------+ |    |    | Subpartition request
 *               |                                         |    |    |
 *               |                      +----------------+ |    |    |
 * +-------+     | +-------------+  +=> | Subpartition 1 | | <==+====+
 * | Map 2 | ==> | | Partition 2 | =|   +----------------+ |    |         +-----------------------+
 * +-------+     | +-------------+  +=> | Subpartition 2 | | <==+======== | Input Gate | Reduce 2 |
 *               |                      +----------------+ |              +-----------------------+
 *               +-----------------------------------------+
 * }</pre>
 *
 * <p> In the above example, two map subtasks produce the intermediate result in parallel, resulting
 * in two partitions (Partition 1 and 2). Each of these partitions is further partitioned into two
 * subpartitions -- one for each parallel reduce subtask. As shown in the Figure, each reduce task
 * will have an input gate attached to it. This will provide its input, which will consist of one
 * subpartition from each partition of the intermediate result.
 */

/**
 * ======================= Task的输入 =======================
 * 这里的主要流程：
 * 核心主要是Task通过循环调用 InputGate.getNextBufferOrEvent 方法获取输入数据，并将获取的数据交给它所封装的算子进行处理，这样就构成了一个Task的基本运行逻辑；
 * getNextBufferOrEvent的流程： (对SingleInputGate来说)
 * 	1. requestPartitions(一次性)：首先让每个 inputChannel 去关联对应的sub-partition， 这涉及到 channel 和 ResultPartitionView 的关联，需要后续再看
 * 	2.
 *
 * ======================= Task的输出 =======================
 *
 * Task 的输入被抽象为 InputGate, 而 InputGate 则由 InputChannel 组成， InputChannel 和该 Task 需要消费的 ResultSubpartition 是一一对应的。
 * Task 通过循环调用 InputGate.getNextBufferOrEvent 方法获取输入数据，并将获取的数据交给它所封装的算子进行处理，这构成了一个 Task 的基本运行逻辑。
 * InputGate 有两个具体的实现，分别为 SingleInputGate 和 UnionInputGate, UnionInputGate 有多个 SingleInputGate 联合构成;
 */
public interface InputGate {

	int getNumberOfInputChannels();

	boolean isFinished();

	//请求消费 ResultPartition
	void requestPartitions() throws IOException, InterruptedException;

	/**
	 * Blocking call waiting for next {@link BufferOrEvent}.
	 * 阻塞调用 获取下一个Buffer或事件
	 * @return {@code Optional.empty()} if {@link #isFinished()} returns true.
	 */
	//Task 通过循环调用 InputGate.getNextBufferOrEvent 方法获取输入数据，并将获取的数据交给它所封装的算子进行处理，这构成了一个 Task 的基本运行逻辑。
	Optional<BufferOrEvent> getNextBufferOrEvent() throws IOException, InterruptedException;

	/**
	 * Poll the {@link BufferOrEvent}.
	 * 非阻塞调用 获取下一个Buffer或事件
	 * @return {@code Optional.empty()} if there is no data to return or if {@link #isFinished()} returns true.
	 */
	Optional<BufferOrEvent> pollNextBufferOrEvent() throws IOException, InterruptedException;

	void sendTaskEvent(TaskEvent event) throws IOException;

	void registerListener(InputGateListener listener);

	int getPageSize();
}
