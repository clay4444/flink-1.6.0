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

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.deployment.InputChannelDeploymentDescriptor;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionLocation;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.taskmanager.TaskActions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Timer;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

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
 * subpartitions -- one for each parallel reduce subtask.
 */
public class SingleInputGate implements InputGate {

	private static final Logger LOG = LoggerFactory.getLogger(SingleInputGate.class);

	/** Lock object to guard partition requests and runtime channel updates. */
	private final Object requestLock = new Object();

	/** The name of the owning task, for logging purposes. */
	private final String owningTaskName;

	/** The job ID of the owning task. */
	private final JobID jobId;

	/**
	 * The ID of the consumed intermediate result. Each input gate consumes partitions of the
	 * intermediate result specified by this ID. This ID also identifies the input gate at the
	 * consuming task.
	 */
	private final IntermediateDataSetID consumedResultId;

	/** The type of the partition the input gate is consuming. */
	private final ResultPartitionType consumedPartitionType;   // ResultPartitionType      blocking / pipline ?

	/**
	 * The index of the consumed subpartition of each consumed partition. This index depends on the
	 * {@link DistributionPattern} and the subtask indices of the producing and consuming task.
	 */
	private final int consumedSubpartitionIndex;

	/** The number of input channels (equivalent to the number of consumed partitions). */
	private final int numberOfInputChannels;   //inputChannel的个数

	/**
	 * Input channels. There is a one input channel for each consumed intermediate result partition.
	 * We store this in a map for runtime updates of single channels.
	 */
	//该 InputGate 包含的所有 InputChannel
	private final Map<IntermediateResultPartitionID, InputChannel> inputChannels;

	/** Channels, which notified this input gate about available data. */
	//InputChannel 构成的队列，这些 InputChannel 中都有可供消费的数据
	private final ArrayDeque<InputChannel> inputChannelsWithData = new ArrayDeque<>();

	/**
	 * Field guaranteeing uniqueness for inputChannelsWithData queue. Both of those fields should be unified
	 * onto one.
	 */
	//保证 inputChannelsWithData 队列唯一性的字段,这两个字段保存的数据应该保持一致。 可以用来快速辅助判断一个channel是否在 inputChannelsWithData 队列中；  值得借鉴！！！
	private final BitSet enqueuedInputChannelsWithData; //可以理解为一个map，key是 channel index，value是 boolean(标记这个channel当前是否有数据)

	private final BitSet channelsWithEndOfPartitionEvents;

	/** The partition state listener listening to failed partition requests. */
	private final TaskActions taskActions;

	/**
	 * Buffer pool for incoming buffers. Incoming data from remote channels is copied to buffers
	 * from this pool.
	 */
	//用于接收输入的缓冲池
	private BufferPool bufferPool;

	/** Global network buffer pool to request and recycle exclusive buffers (only for credit-based). */
	//全局网络缓冲池
	private NetworkBufferPool networkBufferPool;

	private final boolean isCreditBased;

	private boolean hasReceivedAllEndOfPartitionEvents;

	/** Flag indicating whether partitions have been requested. */
	private boolean requestedPartitionsFlag;

	/** Flag indicating whether all resources have been released. */
	private volatile boolean isReleased;

	/** Registered listener to forward buffer notifications to. */
	private volatile InputGateListener inputGateListener;

	private final List<TaskEvent> pendingEvents = new ArrayList<>();

	private int numberOfUninitializedChannels;

	/** Number of network buffers to use for each remote input channel. */
	private int networkBuffersPerChannel;

	/** A timer to retrigger local partition requests. Only initialized if actually needed. */
	private Timer retriggerLocalRequestTimer;

	public SingleInputGate(
		String owningTaskName,
		JobID jobId,
		IntermediateDataSetID consumedResultId,
		final ResultPartitionType consumedPartitionType,
		int consumedSubpartitionIndex,
		int numberOfInputChannels,
		TaskActions taskActions,
		TaskIOMetricGroup metrics,
		boolean isCreditBased) {

		this.owningTaskName = checkNotNull(owningTaskName);
		this.jobId = checkNotNull(jobId);

		this.consumedResultId = checkNotNull(consumedResultId);
		this.consumedPartitionType = checkNotNull(consumedPartitionType);

		checkArgument(consumedSubpartitionIndex >= 0);
		this.consumedSubpartitionIndex = consumedSubpartitionIndex;

		checkArgument(numberOfInputChannels > 0);
		this.numberOfInputChannels = numberOfInputChannels;

		this.inputChannels = new HashMap<>(numberOfInputChannels);
		this.channelsWithEndOfPartitionEvents = new BitSet(numberOfInputChannels);
		this.enqueuedInputChannelsWithData = new BitSet(numberOfInputChannels);

		this.taskActions = checkNotNull(taskActions);
		this.isCreditBased = isCreditBased;
	}

	// ------------------------------------------------------------------------
	// Properties
	// ------------------------------------------------------------------------

	@Override
	public int getNumberOfInputChannels() {
		return numberOfInputChannels;
	}

	public IntermediateDataSetID getConsumedResultId() {
		return consumedResultId;
	}

	/**
	 * Returns the type of this input channel's consumed result partition.
	 *
	 * @return consumed result partition type
	 */
	public ResultPartitionType getConsumedPartitionType() {
		return consumedPartitionType;
	}

	BufferProvider getBufferProvider() {
		return bufferPool;
	}

	public BufferPool getBufferPool() {
		return bufferPool;
	}

	@Override
	public int getPageSize() {
		if (bufferPool != null) {
			return bufferPool.getMemorySegmentSize();
		}
		else {
			throw new IllegalStateException("Input gate has not been initialized with buffers.");
		}
	}

	public int getNumberOfQueuedBuffers() {
		// re-try 3 times, if fails, return 0 for "unknown"
		for (int retry = 0; retry < 3; retry++) {
			try {
				int totalBuffers = 0;

				for (InputChannel channel : inputChannels.values()) {
					if (channel instanceof RemoteInputChannel) {
						totalBuffers += ((RemoteInputChannel) channel).getNumberOfQueuedBuffers();
					}
				}

				return  totalBuffers;
			}
			catch (Exception ignored) {}
		}

		return 0;
	}

	// ------------------------------------------------------------------------
	// Setup/Life-cycle
	// ------------------------------------------------------------------------

	public void setBufferPool(BufferPool bufferPool) {
		checkState(this.bufferPool == null, "Bug in input gate setup logic: buffer pool has" +
			"already been set for this input gate.");

		this.bufferPool = checkNotNull(bufferPool);
	}

	/**
	 * Assign the exclusive buffers to all remote input channels directly for credit-based mode.
	 *
	 * @param networkBufferPool The global pool to request and recycle exclusive buffers
	 * @param networkBuffersPerChannel The number of exclusive buffers for each channel
	 */
	public void assignExclusiveSegments(NetworkBufferPool networkBufferPool, int networkBuffersPerChannel) throws IOException {
		checkState(this.isCreditBased, "Bug in input gate setup logic: exclusive buffers only exist with credit-based flow control.");
		checkState(this.networkBufferPool == null, "Bug in input gate setup logic: global buffer pool has" +
			"already been set for this input gate.");

		this.networkBufferPool = checkNotNull(networkBufferPool);
		this.networkBuffersPerChannel = networkBuffersPerChannel;

		synchronized (requestLock) {
			for (InputChannel inputChannel : inputChannels.values()) {
				if (inputChannel instanceof RemoteInputChannel) {
					((RemoteInputChannel) inputChannel).assignExclusiveSegments(
						networkBufferPool.requestMemorySegments(networkBuffersPerChannel));
				}
			}
		}
	}

	/**
	 * The exclusive segments are recycled to network buffer pool directly when input channel is released.
	 *
	 * @param segments The exclusive segments need to be recycled
	 */
	public void returnExclusiveSegments(List<MemorySegment> segments) throws IOException {
		networkBufferPool.recycleMemorySegments(segments);
	}

	public void setInputChannel(IntermediateResultPartitionID partitionId, InputChannel inputChannel) {
		synchronized (requestLock) {
			if (inputChannels.put(checkNotNull(partitionId), checkNotNull(inputChannel)) == null
					&& inputChannel instanceof UnknownInputChannel) {

				numberOfUninitializedChannels++;
			}
		}
	}

	public void updateInputChannel(InputChannelDeploymentDescriptor icdd) throws IOException, InterruptedException {
		synchronized (requestLock) {
			if (isReleased) {
				// There was a race with a task failure/cancel
				return;
			}

			final IntermediateResultPartitionID partitionId = icdd.getConsumedPartitionId().getPartitionId();

			InputChannel current = inputChannels.get(partitionId);

			if (current instanceof UnknownInputChannel) {

				UnknownInputChannel unknownChannel = (UnknownInputChannel) current;

				InputChannel newChannel;

				ResultPartitionLocation partitionLocation = icdd.getConsumedPartitionLocation();

				if (partitionLocation.isLocal()) {
					newChannel = unknownChannel.toLocalInputChannel();
				}
				else if (partitionLocation.isRemote()) {
					newChannel = unknownChannel.toRemoteInputChannel(partitionLocation.getConnectionId());

					if (this.isCreditBased) {
						checkState(this.networkBufferPool != null, "Bug in input gate setup logic: " +
							"global buffer pool has not been set for this input gate.");
						((RemoteInputChannel) newChannel).assignExclusiveSegments(
							networkBufferPool.requestMemorySegments(networkBuffersPerChannel));
					}
				}
				else {
					throw new IllegalStateException("Tried to update unknown channel with unknown channel.");
				}

				LOG.debug("Updated unknown input channel to {}.", newChannel);

				inputChannels.put(partitionId, newChannel);

				if (requestedPartitionsFlag) {
					newChannel.requestSubpartition(consumedSubpartitionIndex);
				}

				for (TaskEvent event : pendingEvents) {
					newChannel.sendTaskEvent(event);
				}

				if (--numberOfUninitializedChannels == 0) {
					pendingEvents.clear();
				}
			}
		}
	}

	/**
	 * Retriggers a partition request.
	 */
	public void retriggerPartitionRequest(IntermediateResultPartitionID partitionId) throws IOException, InterruptedException {
		synchronized (requestLock) {
			if (!isReleased) {
				final InputChannel ch = inputChannels.get(partitionId);

				checkNotNull(ch, "Unknown input channel with ID " + partitionId);

				LOG.debug("Retriggering partition request {}:{}.", ch.partitionId, consumedSubpartitionIndex);

				if (ch.getClass() == RemoteInputChannel.class) {
					final RemoteInputChannel rch = (RemoteInputChannel) ch;
					rch.retriggerSubpartitionRequest(consumedSubpartitionIndex);
				}
				else if (ch.getClass() == LocalInputChannel.class) {
					final LocalInputChannel ich = (LocalInputChannel) ch;

					if (retriggerLocalRequestTimer == null) {
						retriggerLocalRequestTimer = new Timer(true);
					}

					ich.retriggerSubpartitionRequest(retriggerLocalRequestTimer, consumedSubpartitionIndex);
				}
				else {
					throw new IllegalStateException(
							"Unexpected type of channel to retrigger partition: " + ch.getClass());
				}
			}
		}
	}

	public void releaseAllResources() throws IOException {
		boolean released = false;
		synchronized (requestLock) {
			if (!isReleased) {
				try {
					LOG.debug("{}: Releasing {}.", owningTaskName, this);

					if (retriggerLocalRequestTimer != null) {
						retriggerLocalRequestTimer.cancel();
					}

					for (InputChannel inputChannel : inputChannels.values()) {
						try {
							inputChannel.releaseAllResources();
						}
						catch (IOException e) {
							LOG.warn("Error during release of channel resources: " + e.getMessage(), e);
						}
					}

					// The buffer pool can actually be destroyed immediately after the
					// reader received all of the data from the input channels.
					if (bufferPool != null) {
						bufferPool.lazyDestroy();
					}
				}
				finally {
					isReleased = true;
					released = true;
				}
			}
		}

		if (released) {
			synchronized (inputChannelsWithData) {
				inputChannelsWithData.notifyAll();
			}
		}
	}

	@Override
	public boolean isFinished() {
		synchronized (requestLock) {
			for (InputChannel inputChannel : inputChannels.values()) {
				if (!inputChannel.isReleased()) {
					return false;
				}
			}
		}

		return true;
	}

	//请求分区
	@Override
	public void requestPartitions() throws IOException, InterruptedException {
		synchronized (requestLock) {
			if (!requestedPartitionsFlag) {  //只请求一次
				if (isReleased) {
					throw new IllegalStateException("Already released.");
				}

				// Sanity checks
				if (numberOfInputChannels != inputChannels.size()) {
					throw new IllegalStateException("Bug in input gate setup logic: mismatch between" +
							"number of total input channels and the currently set number of input " +
							"channels.");
				}

				for (InputChannel inputChannel : inputChannels.values()) { //先拿到所有的inputChannel
					//每一个channel都请求对应的子分区
					inputChannel.requestSubpartition(consumedSubpartitionIndex); //所以让每一个channel去请求对应的 sub-partition，  具体实现需要后面再看
				}
			}

			requestedPartitionsFlag = true;  //已经请求过了，
		}
	}

	// ------------------------------------------------------------------------
	// Consume
	// ------------------------------------------------------------------------

	//阻塞调用获取下一个输入
	@Override
	public Optional<BufferOrEvent> getNextBufferOrEvent() throws IOException, InterruptedException {
		return getNextBufferOrEvent(true);
	}

	//非阻塞调用下一个输入
	@Override
	public Optional<BufferOrEvent> pollNextBufferOrEvent() throws IOException, InterruptedException {
		return getNextBufferOrEvent(false);
	}

	/**
	 * 阻塞和非阻塞都是调用的这个；
	 * 
	 */
	private Optional<BufferOrEvent> getNextBufferOrEvent(boolean blocking) throws IOException, InterruptedException {
		if (hasReceivedAllEndOfPartitionEvents) {
			return Optional.empty();
		}

		if (isReleased) {
			throw new IllegalStateException("Released");
		}

		//首先尝试请求分区，实际的请求只会执行一次
		requestPartitions();

		InputChannel currentChannel;
		boolean moreAvailable;
		Optional<BufferAndAvailability> result = Optional.empty();

		do {
			synchronized (inputChannelsWithData) {  //以这个只包含所有有数据的channel的队列为锁，(回想生产者-消费者模型)

				//从 inputChannelsWithData 队列中获取有数据的 channel，经典的生产者-消费者模式;   inputChannelsWithData是所有inputChannel组成的队列；
				while (inputChannelsWithData.size() == 0) {  //说明所有的channel都没有新数据； 也就是说 inputChannelsWithData 中包含的是所有有数据可供消费的inputChannel
					if (isReleased) {
						throw new IllegalStateException("Released");
					}

					if (blocking) {
						inputChannelsWithData.wait();  // wait()  阻塞等待
					}
					else {
						return Optional.empty();
					}
				}
				//出了上面的while循环，说明有一个inputChannel有数据了；

				currentChannel = inputChannelsWithData.remove(); //从inputChannelsWithData中移除(假设只有一条数据)，赋给 currentChannel
				enqueuedInputChannelsWithData.clear(currentChannel.getChannelIndex());
				moreAvailable = inputChannelsWithData.size() > 0; //是否还有其他inputChannel有数据可消费；
			}

			result = currentChannel.getNextBuffer(); //获取一个buffer
		} while (!result.isPresent());  //result没值，就一直在这循环获取

		// this channel was now removed from the non-empty channels queue
		// we re-add it in case it has more data, because in that case no "non-empty" notification
		// will come for that channel
		if (result.get().moreAvailable()) { //如果上一个channel有不止一条数据的话，
			queueChannel(currentChannel);	//就把它加回 inputChannelsWithData 队列中，以便下次继续消费
			moreAvailable = true;
		}

		final Buffer buffer = result.get().buffer();
		if (buffer.isBuffer()) {
			return Optional.of(new BufferOrEvent(buffer, currentChannel.getChannelIndex(), moreAvailable));
		}
		else {
			final AbstractEvent event = EventSerializer.fromBuffer(buffer, getClass().getClassLoader());

			if (event.getClass() == EndOfPartitionEvent.class) {
				channelsWithEndOfPartitionEvents.set(currentChannel.getChannelIndex());

				if (channelsWithEndOfPartitionEvents.cardinality() == numberOfInputChannels) {
					// Because of race condition between:
					// 1. releasing inputChannelsWithData lock in this method and reaching this place
					// 2. empty data notification that re-enqueues a channel
					// we can end up with moreAvailable flag set to true, while we expect no more data.
					checkState(!moreAvailable || !pollNextBufferOrEvent().isPresent());
					moreAvailable = false;
					hasReceivedAllEndOfPartitionEvents = true;
				}

				currentChannel.notifySubpartitionConsumed();

				currentChannel.releaseAllResources();
			}

			return Optional.of(new BufferOrEvent(event, currentChannel.getChannelIndex(), moreAvailable));
		}
	}

	@Override
	public void sendTaskEvent(TaskEvent event) throws IOException {
		synchronized (requestLock) {
			for (InputChannel inputChannel : inputChannels.values()) {
				inputChannel.sendTaskEvent(event);
			}

			if (numberOfUninitializedChannels > 0) {
				pendingEvents.add(event);
			}
		}
	}

	// ------------------------------------------------------------------------
	// Channel notifications
	// ------------------------------------------------------------------------

	//监控InputGate是否有数据的监听器
	@Override
	public void registerListener(InputGateListener inputGateListener) {
		if (this.inputGateListener == null) {
			this.inputGateListener = inputGateListener;
		} else {
			throw new IllegalStateException("Multiple listeners");
		}
	}

	void notifyChannelNonEmpty(InputChannel channel) {
		queueChannel(checkNotNull(channel));
	}

	void triggerPartitionStateCheck(ResultPartitionID partitionId) {
		taskActions.triggerPartitionProducerStateCheck(jobId, consumedResultId, partitionId);
	}

	//把一个有数据的channel加入 inputChannelsWithData 队列中；
	private void queueChannel(InputChannel channel) {
		int availableChannels;

		synchronized (inputChannelsWithData) { //也用  inputChannelsWithData 为锁
			if (enqueuedInputChannelsWithData.get(channel.getChannelIndex())) { //已经在队列中了，直接返回
				return;
			}

			//否则，加入队列；

			availableChannels = inputChannelsWithData.size(); //注意这里是加入队列之前获取的

			inputChannelsWithData.add(channel);  //入队
			enqueuedInputChannelsWithData.set(channel.getChannelIndex());  //标记对应channel已在队列中

			if (availableChannels == 0) {  //为什么=0的时候notify呢？  因为availableChannels是新channel进队列之前取的，也就是说之前没有channel有数据，所以消费者阻塞了，现在有数据了，需要唤醒
				inputChannelsWithData.notifyAll(); //在这进行的 notifyAll 唤醒；
			}
		}

		if (availableChannels == 0) {
			InputGateListener listener = inputGateListener;
			if (listener != null) {
				listener.notifyInputGateNonEmpty(this); //通知InputGate监听器有数据可以消费了
			}
		}
	}

	// ------------------------------------------------------------------------

	Map<IntermediateResultPartitionID, InputChannel> getInputChannels() {
		return inputChannels;
	}

	// ------------------------------------------------------------------------

	/**
	 * Creates an input gate and all of its input channels.
	 */
	public static SingleInputGate create(
		String owningTaskName,
		JobID jobId,
		ExecutionAttemptID executionId,
		InputGateDeploymentDescriptor igdd,
		NetworkEnvironment networkEnvironment,
		TaskActions taskActions,
		TaskIOMetricGroup metrics) {

		final IntermediateDataSetID consumedResultId = checkNotNull(igdd.getConsumedResultId());
		final ResultPartitionType consumedPartitionType = checkNotNull(igdd.getConsumedPartitionType());

		final int consumedSubpartitionIndex = igdd.getConsumedSubpartitionIndex();
		checkArgument(consumedSubpartitionIndex >= 0);

		final InputChannelDeploymentDescriptor[] icdd = checkNotNull(igdd.getInputChannelDeploymentDescriptors());

		final SingleInputGate inputGate = new SingleInputGate(
			owningTaskName, jobId, consumedResultId, consumedPartitionType, consumedSubpartitionIndex,
			icdd.length, taskActions, metrics, networkEnvironment.isCreditBased());

		// Create the input channels. There is one input channel for each consumed partition.
		final InputChannel[] inputChannels = new InputChannel[icdd.length];

		int numLocalChannels = 0;
		int numRemoteChannels = 0;
		int numUnknownChannels = 0;

		for (int i = 0; i < inputChannels.length; i++) {
			final ResultPartitionID partitionId = icdd[i].getConsumedPartitionId();
			final ResultPartitionLocation partitionLocation = icdd[i].getConsumedPartitionLocation();

			if (partitionLocation.isLocal()) {
				inputChannels[i] = new LocalInputChannel(inputGate, i, partitionId,
					networkEnvironment.getResultPartitionManager(),
					networkEnvironment.getTaskEventDispatcher(),
					networkEnvironment.getPartitionRequestInitialBackoff(),
					networkEnvironment.getPartitionRequestMaxBackoff(),
					metrics
				);

				numLocalChannels++;
			}
			else if (partitionLocation.isRemote()) {
				inputChannels[i] = new RemoteInputChannel(inputGate, i, partitionId,
					partitionLocation.getConnectionId(),
					networkEnvironment.getConnectionManager(),
					networkEnvironment.getPartitionRequestInitialBackoff(),
					networkEnvironment.getPartitionRequestMaxBackoff(),
					metrics
				);

				numRemoteChannels++;
			}
			else if (partitionLocation.isUnknown()) {
				inputChannels[i] = new UnknownInputChannel(inputGate, i, partitionId,
					networkEnvironment.getResultPartitionManager(),
					networkEnvironment.getTaskEventDispatcher(),
					networkEnvironment.getConnectionManager(),
					networkEnvironment.getPartitionRequestInitialBackoff(),
					networkEnvironment.getPartitionRequestMaxBackoff(),
					metrics
				);

				numUnknownChannels++;
			}
			else {
				throw new IllegalStateException("Unexpected partition location.");
			}

			inputGate.setInputChannel(partitionId.getPartitionId(), inputChannels[i]);
		}

		LOG.debug("Created {} input channels (local: {}, remote: {}, unknown: {}).",
			inputChannels.length,
			numLocalChannels,
			numRemoteChannels,
			numUnknownChannels);

		return inputGate;
	}
}
