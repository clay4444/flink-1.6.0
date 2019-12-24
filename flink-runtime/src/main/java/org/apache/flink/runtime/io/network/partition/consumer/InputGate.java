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
 * 	1. requestPartitions(一次性)：首先让每个 inputChannel 去关联对应的sub-partition， 这涉及到 channel 和 ResultPartitionView 的关联，需要后续再看(具体实现看LocalInputChannel类 / RemoteInputChannel类 的注释)
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
 *  > 后续 -> LocalInputChannel (具体实现点进去看)
 *   1.requestPartitions的实现就是通过partitionManager创建了一个ResultSubpartitionView，然后自己自己作为监听器传进去了，当上游ResultSubPartition有数据写入ResultSubpartitionView时，
 *     会回调触发notifyChannelNonEmpty(channel)方法，触发生产者的逻辑；
 *   2.getNextBuffer()主要就是通过ResultSubpartitionView去消费数据的；
 *
 *  > 基于网络 (Netty) 的数据交换  -> RemoteInputChannel (具体实现点进去看)
 *   0.Netty的启动过程在NettyConnectionManager类中有详细的注释，这里不再赘述，注意tm刚启动的时候，NettyConnectionManager就启动了(NettyServer就启动了) 即可
 *   1.requestPartitions()的实现是使用NettyClient去和远程NettyServer建立连接，返回一个tcpChannel，代表一个TCP连接；然后再通过这个连接发送PartitionRequest请求, (结果Future直接忽略了，为啥？)
 *   2.getNextBuffer()的实现就是直接从receivedBuffers中取的，那receivedBuffers中的数据从哪里来的呢？
 * ======================= 问题： =======================
 *
 * ======================= 流量控制 =======================
 *
 * Flink 在两个 Task 之间建立 Netty 连接进行数据传输，每一个 Task 会分配两个缓冲池，一个用于输出数据，一个用于接收数据。当一个 Task 的缓冲池用尽之后，网络连接就处于阻塞状态，上游 Task 无法产出数据，
 * 下游 Task 无法接收数据，也就是我们所说的“反压”状态。这是一种非常自然的“反压”的机制，但是过程也相对比较粗暴。由于 TaskManager 之间的网络连接是由不同 Task 复用的，一旦网络处于阻塞状态，
 * 所有 Task 都无法向 TCP 连接中写入数据或者从中读取数据，即便其它 Task 关联的缓冲池仍然存在空余。此外，由于网络发生了阻塞，诸如 CheckpointBarrier 等事件也无法在 Task 之间进行流转。
 *
 * 为了解决上述问题，Flink 1.5 重构了网络栈，引入了“基于信用值的流量控制算法”（Credit-based Flow Control），确保 TaskManager 之间的网络连接始终不会处于阻塞状态。
 * Credit-based Flow Control 的思路其实也比较简单，它是在接收端和发送端之间建立一种类似“信用评级”的机制，发送端向接收端发送的数据永远不会超过接收端的信用值的大小。
 * 在 Flink 这里，信用值就是接收端可用的 Buffer 的数量，这样就可以保证发送端不会向 TCP 连接中发送超出接收端缓冲区可用容量的数据。
 *
 * 相比于之前所有的 InputChannel 共享同一个本地缓冲池的方式，在重构网络栈之后，Flink 会为每一个 InputChannel 分配一批独占的缓冲（exclusive buffers），而本地缓冲池中的 buffer 则作为流动的（floating buffers），可以被所有的 InputChannel 使用。
 *
 * Credit-based Flow Control 的具体机制为：
 *  1.接收端向发送端声明可用的 Credit（一个可用的 buffer 对应一点 credit）；
 *  2.当发送端获得了 X 点 Credit，表明它可以向网络中发送 X 个 buffer；当接收端分配了 X 点 Credit 给发送端，表明它有 X 个空闲的 buffer 可以接收数据；
 *  3.只有在 Credit > 0 的情况下发送端才发送 buffer；发送端每发送一个 buffer，Credit 也相应地减少一点
 *  4.由于 CheckpointBarrier，EndOfPartitionEvent 等事件可以被立即处理，因而事件可以立即发送，无需使用 Credit
 *  5.当发送端发送 buffer 的时候，它同样把当前堆积的 buffer 数量（backlog size）告知接收端；接收端根据发送端堆积的数量来申请 floating buffer
 *
 * 这种流量控制机制可以有效地改善网络的利用率，不会因为 buffer 长时间停留在网络链路中进而导致整个所有的 Task 都无法继续处理数据，也无法进行 Checkpoint 操作。但是它的一个潜在的缺点是增加了上下游之间的通信成本（需要发送 credit 和 backlog 信息）。在目前的版本中可以通过 taskmanager.network.credit-model: false 来禁用，但后续应该会移除这个配置项。
 *
 * 具体实现：(详细的源码解析在相应的实现类中)
 *  1.初始化：Task刚启动时向NetworkEnvironment注册Task，然后setupInputGate()为Task的输入配置buffer资源，资源又分为独占的共享的，这两种资源都放进了RemoteInputChannel的 AvailableBufferQueue 这个容器中；
 *  2.请求远端子分区，上面已经解析过了，也可以看 RemoteInputChannel#requestSubpartition() 源码， 核心是通过NettyClient发送PartitionRequest请求，
 *  3.生产端的处理流程，主要是两个handler:
 *  	3.1 PartitionRequestServerHandler 用来接收PartitionRequest，然后构建一个CreditBasedSequenceNumberingViewReader，简称reader，reader负责连接这个request要请求的ResultSubPartitionView，并且在~View中有数据时收到通知；
 *  	3.2 PartitionRequestQueue 用来监听chnnel的状态，当可写入时，就从上述的reader中取出数据并写入到channel中(发送给客户端)
 *  	3.3 这两个handler之间通过一个队列来连接，即PartitionRequestQueue#availableReaders，reader中有数据时，写入这个队列，作为生产者，PartitionRequestQueue监听到channel可写时，也是从这个队列中消费，作为消费者；
 *  	3.4 详细的源码分析在内两个具体的类中
 *  4.消费者的处理流程，
 *
 *
 *
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
