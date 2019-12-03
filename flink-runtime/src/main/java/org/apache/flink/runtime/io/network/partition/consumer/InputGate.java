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
 * getNextBufferOrEvent的流程： (对 SingleInputGate 来说)
 * 	1. requestPartitions(一次性)：首先让每个 inputChannel 去关联对应的sub-partition， 这涉及到 channel 和 ResultPartitionView 的关联，需要后续再看
 * 	2. SingleInputGate通过维护一个队列(只包含所有有数据的inputChannel)形成了一个生产者消费者设计模式，
 * 	3. 进入getNextBufferOrEvent()方法，开始消费者的逻辑，从inputChannel中消费数据，消费到了，就返回。消费出一条之后，还会判断是否有更多数据，如果还有，就触发queueChannel的逻辑
 * 	4. queueChannel其中就是生产者的逻辑，把一个有数据的inputChannel加入到队列中，通知阻塞的消费者 (还会通知inputGate监听器)
 * 	5. 除了消费的时候会触发queueChannel的逻辑，notifyChannelNonEmpty(channel)方法也会触发，这个是类似回调的方式，通知当前InputGate有新的InputChannel有新数据可以消费了；
 * 整个流程就是从SingleInputGate中获取数据的流程；
 *
 * (对 UnionInputGate 来说)
 * 整体过程和SingleInputGate基本是一致的，只是队列中的数据不再是InputChannel，而是SingleInputGate，所以UnionInputGate就可以理解为是由几个SingleInputGate组成的
 * 其他过程都是完全一致的；
 *
 * ======================= Task的输入 =======================
 *
 * Task 的输入被抽象为 InputGate, 而 InputGate 则由 InputChannel 组成， InputChannel 和该 Task 需要消费的 ResultSubpartition 是一一对应的。
 * Task 通过循环调用 InputGate.getNextBufferOrEvent 方法获取输入数据，并将获取的数据交给它所封装的算子进行处理，这构成了一个 Task 的基本运行逻辑。
 * InputGate 有两个具体的实现，分别为 SingleInputGate 和 UnionInputGate, UnionInputGate 有多个 SingleInputGate 联合构成;
 *
 * InputGate 实际上可以理解为是对 InputChannel 的一层封装，实际数据的获取还是要依赖于 InputChannel。
 *
 * ======================= 问题： =======================
 * 数据输出的逻辑已经清楚了，通过RecordWriter最终写入到ResultSubPartition，然后通知SubpartitionView，
 * 数据输入的逻辑也清楚了，通过InputGate(内部维护一个生产者消费者的逻辑)，最终从 InputChannel 中循环读取buffer数据；
 * 这两者怎么结合起来的呢？
 *
 * InputChannel有两种不同的实现：LocalInputChannel 和 RemoteInputChannel，分别对应本地和远程数据交换；
 *  > 后续 -> LocalInputChannel
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
