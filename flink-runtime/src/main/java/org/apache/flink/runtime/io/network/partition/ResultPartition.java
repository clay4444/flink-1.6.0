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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferPoolOwner;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.partition.consumer.LocalInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.taskmanager.TaskActions;
import org.apache.flink.runtime.taskmanager.TaskManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkElementIndex;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A result partition for data produced by a single task.
 *
 * <p> This class is the runtime part of a logical {@link IntermediateResultPartition}. Essentially,
 * a result partition is a collection of {@link Buffer} instances. The buffers are organized in one
 * or more {@link ResultSubpartition} instances, which further partition the data depending on the
 * number of consuming tasks and the data {@link DistributionPattern}.
 *
 * <p> Tasks, which consume a result partition have to request one of its subpartitions. The request
 * happens either remotely (see {@link RemoteInputChannel}) or locally (see {@link LocalInputChannel})
 *
 * <h2>Life-cycle</h2>
 *
 * The life-cycle of each result partition has three (possibly overlapping) phases:
 * <ol>
 * <li><strong>Produce</strong>: </li>
 * <li><strong>Consume</strong>: </li>
 * <li><strong>Release</strong>: </li>
 * </ol>
 *
 * <h2>Lazy deployment and updates of consuming tasks</h2>
 *
 * Before a consuming task can request the result, it has to be deployed. The time of deployment
 * depends on the PIPELINED vs. BLOCKING characteristic of the result partition. With pipelined
 * results, receivers are deployed as soon as the first buffer is added to the result partition.
 * With blocking results on the other hand, receivers are deployed after the partition is finished.
 *
 * <h2>Buffer management</h2>
 *
 * <h2>State management</h2>
 */

/**
 *  ======================= Task的输出 =======================
 *  这里涉及的流程：
 *  1. tm启动时创建一个 NetworkEnvironment，并调用start()方法来启动，用来管理这个tm上的所有网络资源环境；
 *  2. Task启动前，向所在tm上的 NetworkEnvironment#registerTask 来注册当前task，NetworkEnvironment会回调这个task的ResultPartition的registerBufferPool方法，来为ResultPartition(代表这个task的输出)设置输出数据需要的资源；
 *		2.1 这里还会把Task的所有ResultPartition都注册到resultPartitionManager中，所以resultPartitionManager管理这个tm上的所有 ResultPartition(一个Task一个，所以可能会有多个)
 *
 *  上面已经设置好了资源，那task要怎么往后发送数据呢？
 *  3. task启动，创建一个RecordWriter，然后开始调用emit方法往下游发送数据，
 *  	3.1 通过 ChannelSelector 确定写入的目标 channel
 * 		3.2 找到目标channel对应的序列化器，使用 RecordSerializer 对记录进行序列化
 * 		3.3 向 ResultPartition 请求 BufferBuilder，用于写入序列化结果
 * 		3.4 向 ResultPartition 添加 BufferConsumer，用于读取写入 Buffer , 写完就读取？ 什么意思？ 这里可以理解为就是把一个可以读取的buffer给了 sub-partition，忽略consumer这个概念；
 *	4. 调用subpartition#add把数据加到本身的buffer队列中，然后通知 PipelinedSubpartitionView 数据产生，可以被消费了(只有第一个buffer会通知)
 *  5. 那么这个 SubpartitionView 是什么时候，怎么产生的呢？是由 resultPartitionManager#createSubpartitionView 创建的(并和对应的sub-partition关联)，创建的时候传入一个监听器，当对应的sub-partition产生数据通知的时候，回调这个监听器的方法；
 *
 *  至此，我们已经了解了一个 Task 如何输出结果到 ResultPartition 中，以及如何去消费不同 ResultSubpartition 中的这些用于保存序列化结果的 Buffer。
 *  ======================= Task的输出 =======================
 *
 * ExecutionGraph 中的 ExecutionVertex 对应最终的 Task； 这个好理解；
 * ExecutionGraph 中的 IntermediateResultPartition(代表最终的一个Task的具体输出)，对应实际执行图中的 ResultPartition(代表一个Task的具体输出)
 * 个人总结：一个Task肯定对应一个 ResultPartition，而ResultPartition中 subPartition 的数量取决于下游有多少个算子要从这个Task的ResultPartition消费数据；
 * 所以它的数量最终取决于下游算子的数量 和 上下游算子之间的数据分发模式，
 *
 * 关于InputGate和InputChannel的理解：
 * InputGate好理解，它代表一个Task的输入，所以每个Task都对应一个InputGate；
 * InputChannel 属于 InputGate，它代表从上游的一个subPartition到当前Task的一个消费管道，所以当前task从上游几个task上消费数据，当前InputGate就有几个InputChannel；
 * InputChannel 和 ExecutionEdge 一一对应
 *
 * 每一个 ResultPartition 都有一个关联的 ResultPartitionWriter; 也都有一个独立的 LocalBufferPool 负责提供写入数据所需的 buffer
 * ResultPartition 实现了 ResultPartitionWriter 接口；
 *
 * ResultPartition 中包含所有的 ResultSubPartition，ResultPartition 创建的时候就顺便创建了这个ResultPartition包含的所有的ResultSubPartition；
 */
public class ResultPartition implements ResultPartitionWriter, BufferPoolOwner {

	private static final Logger LOG = LoggerFactory.getLogger(ResultPartition.class);
	
	private final String owningTaskName;

	private final TaskActions taskActions;

	private final JobID jobId;

	private final ResultPartitionID partitionId;

	/** Type of this partition. Defines the concrete subpartition implementation to use. */
	private final ResultPartitionType partitionType;

	/** The subpartitions of this partition. At least one. */
	// ResultPartition 由 ResultSubpartition 构成，
	// ResultSubpartition 的数量由下游消费 Task 数和 DistributionPattern(ALL_TO_ALL、POINTWISE) 来决定。  数据分发模式，决定上游算子的 subtask 如何连接到下游算子的 subtask
	// 例如，如果是 FORWARD，则下游只有一个消费者；如果是 SHUFFLE，则下游消费者的数量和下游算子的并行度一样
	private final ResultSubpartition[] subpartitions;

	//ResultPartitionManager 管理当前 TaskManager 所有的 ResultPartition
	private final ResultPartitionManager partitionManager;

	//通知当前ResultPartition有数据可供消费的回调函数回调
	private final ResultPartitionConsumableNotifier partitionConsumableNotifier;

	public final int numTargetKeyGroups;

	//在有数据产出时，是否需要发送消息来调度或更新消费者（Stream模式下调度模式为 ScheduleMode.EAGER，无需发通知）
	private final boolean sendScheduleOrUpdateConsumersMessage;

	// - Runtime state --------------------------------------------------------

	private final AtomicBoolean isReleased = new AtomicBoolean();

	/**
	 * The total number of references to subpartitions of this result. The result partition can be
	 * safely released, iff the reference count is zero. A reference count of -1 denotes that the
	 * result partition has been released.
	 */
	private final AtomicInteger pendingReferences = new AtomicInteger();

	private BufferPool bufferPool;

	//是否已经通知了消费者
	private boolean hasNotifiedPipelinedConsumers;

	private boolean isFinished;

	private volatile Throwable cause;

	/**
	 * 构建这个 Task 的 ResultPartition；
	 */
	public ResultPartition(
		String owningTaskName,
		TaskActions taskActions, // actions on the owning task
		JobID jobId,
		ResultPartitionID partitionId,
		ResultPartitionType partitionType,  //partition 类型：BLOCKING、PIPELINED、PIPELINED_BOUNDED
		int numberOfSubpartitions,			//有几个 sub-partition， sub-partition 对应 inputChannel
		int numTargetKeyGroups,
		ResultPartitionManager partitionManager,
		ResultPartitionConsumableNotifier partitionConsumableNotifier,
		IOManager ioManager,
		boolean sendScheduleOrUpdateConsumersMessage) { //在有数据产出时，是否需要发送消息来调度或更新消费者（Stream模式下调度模式为 ScheduleMode.EAGER，无需发通知）

		this.owningTaskName = checkNotNull(owningTaskName);
		this.taskActions = checkNotNull(taskActions);
		this.jobId = checkNotNull(jobId);
		this.partitionId = checkNotNull(partitionId);
		this.partitionType = checkNotNull(partitionType);
		this.subpartitions = new ResultSubpartition[numberOfSubpartitions];
		this.numTargetKeyGroups = numTargetKeyGroups;
		this.partitionManager = checkNotNull(partitionManager);
		this.partitionConsumableNotifier = checkNotNull(partitionConsumableNotifier);
		this.sendScheduleOrUpdateConsumersMessage = sendScheduleOrUpdateConsumersMessage;

		// Create the subpartitions.
		switch (partitionType) {
			// Batch 模式，SpillableSubpartition，在 Buffer 不充足时将结果写入磁盘;
			case BLOCKING:
				for (int i = 0; i < subpartitions.length; i++) {
					subpartitions[i] = new SpillableSubpartition(i, this, ioManager);  //构建所有的sub-partition
				}

				break;

			// Streaming 模式，PipelinedSubpartition
			case PIPELINED:
			case PIPELINED_BOUNDED:
				for (int i = 0; i < subpartitions.length; i++) {
					subpartitions[i] = new PipelinedSubpartition(i, this);    //构建所有的sub-partition， 和上面的不同
				}

				break;

			default:
				throw new IllegalArgumentException("Unsupported result partition type.");
		}

		// Initially, partitions should be consumed once before release.
		pin();

		LOG.debug("{}: Initialized {}", owningTaskName, this);
	}

	/**
	 * Registers a buffer pool with this result partition.
	 * <p>
	 * There is one pool for each result partition, which is shared by all its sub partitions.
	 * <p>
	 * The pool is registered with the partition *after* it as been constructed in order to conform
	 * to the life-cycle of task registrations in the {@link TaskManager}.
	 */

	//Task启动的时候向 NetworkEnvironment 注册，然后NetworkEnvironment通过回调这个方法来为这个Task的ResultPartition(代表task的输出) 设置需要的内存资源；
	public void registerBufferPool(BufferPool bufferPool) {
		checkArgument(bufferPool.getNumberOfRequiredMemorySegments() >= getNumberOfSubpartitions(),
				"Bug in result partition setup logic: Buffer pool has not enough guaranteed buffers for this result partition.");

		checkState(this.bufferPool == null, "Bug in result partition setup logic: Already registered buffer pool.");

		this.bufferPool = checkNotNull(bufferPool);

		// If the partition type is back pressure-free, we register with the buffer pool for
		// callbacks to release memory.
		if (!partitionType.hasBackPressure()) {
			bufferPool.setBufferPoolOwner(this);
		}
	}

	public JobID getJobId() {
		return jobId;
	}

	public ResultPartitionID getPartitionId() {
		return partitionId;
	}

	@Override
	public int getNumberOfSubpartitions() {
		return subpartitions.length;
	}

	@Override
	public BufferProvider getBufferProvider() {
		return bufferPool;
	}

	public BufferPool getBufferPool() {
		return bufferPool;
	}

	public int getNumberOfQueuedBuffers() {
		int totalBuffers = 0;

		for (ResultSubpartition subpartition : subpartitions) {
			totalBuffers += subpartition.unsynchronizedGetNumberOfQueuedBuffers();
		}

		return totalBuffers;
	}

	/**
	 * Returns the type of this result partition.
	 *
	 * @return result partition type
	 */
	public ResultPartitionType getPartitionType() {
		return partitionType;
	}

	// ------------------------------------------------------------------------

	//被RecordWriter调用，把一个bufferConsumer(用于读取写入到 MemorySegment 的数据) 交给对应的 sub-partition，
	//其实也就相当于把一个完整的，可以读取buffer交给了 sub-partition，
	@Override
	public void addBufferConsumer(BufferConsumer bufferConsumer, int subpartitionIndex) throws IOException {
		//向指定的 subpartition 添加一个 buffer
		checkNotNull(bufferConsumer);

		ResultSubpartition subpartition;
		try {
			checkInProduceState();
			subpartition = subpartitions[subpartitionIndex];  //这里也印证了 sub-partition 和 channel 一一对应
		}
		catch (Exception ex) {
			bufferConsumer.close();
			throw ex;
		}

		//添加 BufferConsumer，说明已经有数据生成了
		if (subpartition.add(bufferConsumer)) {  //交给 sub-partition，
			notifyPipelinedConsumers();  //这里，然后通知 pipline 模式下的下游消费者
		}
	}

	@Override
	public void flushAll() {
		for (ResultSubpartition subpartition : subpartitions) {
			subpartition.flush();
		}
	}

	@Override
	public void flush(int subpartitionIndex) {
		subpartitions[subpartitionIndex].flush();
	}

	/**
	 * Finishes the result partition.
	 *
	 * <p> After this operation, it is not possible to add further data to the result partition.
	 *
	 * <p> For BLOCKING results, this will trigger the deployment of consuming tasks.
	 */
	public void finish() throws IOException {
		boolean success = false;

		try {
			checkInProduceState();

			for (ResultSubpartition subpartition : subpartitions) {
				subpartition.finish();
			}

			success = true;
		}
		finally {
			if (success) {
				isFinished = true;

				notifyPipelinedConsumers();
			}
		}
	}

	public void release() {
		release(null);
	}

	/**
	 * Releases the result partition.
	 */
	public void release(Throwable cause) {
		if (isReleased.compareAndSet(false, true)) {
			LOG.debug("{}: Releasing {}.", owningTaskName, this);

			// Set the error cause
			if (cause != null) {
				this.cause = cause;
			}

			// Release all subpartitions
			for (ResultSubpartition subpartition : subpartitions) {
				try {
					subpartition.release();
				}
				// Catch this in order to ensure that release is called on all subpartitions
				catch (Throwable t) {
					LOG.error("Error during release of result subpartition: " + t.getMessage(), t);
				}
			}
		}
	}

	public void destroyBufferPool() {
		if (bufferPool != null) {
			bufferPool.lazyDestroy();
		}
	}

	/**
	 * Returns the requested subpartition.
	 */
	//创建某个sub-partition的 SubpartitionView，用来消费数据；
	public ResultSubpartitionView createSubpartitionView(int index, BufferAvailabilityListener availabilityListener) throws IOException {
		int refCnt = pendingReferences.get();

		checkState(refCnt != -1, "Partition released.");
		checkState(refCnt > 0, "Partition not pinned.");

		checkElementIndex(index, subpartitions.length, "Subpartition not found.");

		ResultSubpartitionView readView = subpartitions[index].createReadView(availabilityListener);  //靠，最终还是调用到 sub-partition 自己的 createReadView 方法；

		LOG.debug("Created {}", readView);

		return readView;
	}

	public Throwable getFailureCause() {
		return cause;
	}

	@Override
	public int getNumTargetKeyGroups() {
		return numTargetKeyGroups;
	}

	/**
	 * Releases buffers held by this result partition.
	 *
	 * <p> This is a callback from the buffer pool, which is registered for result partitions, which
	 * are back pressure-free.
	 */
	@Override
	public void releaseMemory(int toRelease) throws IOException {
		checkArgument(toRelease > 0);

		for (ResultSubpartition subpartition : subpartitions) {
			toRelease -= subpartition.releaseMemory();

			// Only release as much memory as needed
			if (toRelease <= 0) {
				break;
			}
		}
	}

	@Override
	public String toString() {
		return "ResultPartition " + partitionId.toString() + " [" + partitionType + ", "
				+ subpartitions.length + " subpartitions, "
				+ pendingReferences + " pending references]";
	}

	// ------------------------------------------------------------------------

	/**
	 * Pins the result partition.
	 *
	 * <p> The partition can only be released after each subpartition has been consumed once per pin
	 * operation.
	 */
	void pin() {
		while (true) {
			int refCnt = pendingReferences.get();

			if (refCnt >= 0) {
				if (pendingReferences.compareAndSet(refCnt, refCnt + subpartitions.length)) {
					break;
				}
			}
			else {
				throw new IllegalStateException("Released.");
			}
		}
	}

	/**
	 * Notification when a subpartition is released.
	 */
	void onConsumedSubpartition(int subpartitionIndex) {

		if (isReleased.get()) {
			return;
		}

		int refCnt = pendingReferences.decrementAndGet();

		if (refCnt == 0) {
			partitionManager.onConsumedPartition(this);
		}
		else if (refCnt < 0) {
			throw new IllegalStateException("All references released.");
		}

		LOG.debug("{}: Received release notification for subpartition {} (reference count now at: {}).",
				this, subpartitionIndex, pendingReferences);
	}

	ResultSubpartition[] getAllPartitions() {
		return subpartitions;
	}

	// ------------------------------------------------------------------------

	private void checkInProduceState() throws IllegalStateException {
		checkState(!isFinished, "Partition already finished.");
	}

	/**
	 * Notifies pipelined consumers of this result partition once.
	 */
	private void notifyPipelinedConsumers() {
		//对于 Streaming 模式的任务，由于调度模式为 EAGER，所有的 task 都已经部署了，下面的通知不会触发
		if (sendScheduleOrUpdateConsumersMessage && !hasNotifiedPipelinedConsumers && partitionType.isPipelined()) {
			//对于 PIPELINE 类型的 ResultPartition，在第一条记录产生时，
			//会告知 JobMaster 当前 ResultPartition 可被消费，这会触发下游消费者 Task 的部署
			partitionConsumableNotifier.notifyPartitionConsumable(jobId, partitionId, taskActions);

			hasNotifiedPipelinedConsumers = true;
		}
	}
}
