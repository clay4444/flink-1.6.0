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
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;
import java.util.Timer;
import java.util.TimerTask;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An input channel, which requests a local subpartition.
 *
 * 对应InputChannel和上游的 ResultSubpartition 存在同一个tm的情况(也就是两个Task在同一个tm上)
 * 此时它们之间的数据交换就在同一个 JVM 进程内不同线程之间进行，无需通过网络交换
 * ResultSubpartition 中的 buffer 可以通过 ResultSubpartitionView 进行消费
 *
 * 这里的逻辑相对比较简单，LocalInputChannel 实现了 InputChannel 接口，同时也实现了 BufferAvailabilityListener 接口。
 * LocalInputChannel 通过 ResultPartitionManager 请求创建和指定 ResultSubparition 关联的 ResultSubparitionView，并以自身作为 ResultSubparitionView 的回调。
 * 这样，一旦 ResultSubparition 有数据产出时，ResultSubparitionView 会得到通知，同时 LocalInputChannel 的回调函数也会被调用，这样消费者这一端就可以及时获取到数据的生产情况，从而及时地去消费数据。
 */
public class LocalInputChannel extends InputChannel implements BufferAvailabilityListener {

	private static final Logger LOG = LoggerFactory.getLogger(LocalInputChannel.class);

	// ------------------------------------------------------------------------

	private final Object requestLock = new Object();

	/** The local partition manager. */
	private final ResultPartitionManager partitionManager;

	/** Task event dispatcher for backwards events. */
	private final TaskEventDispatcher taskEventDispatcher;

	/** The consumed subpartition */
	private volatile ResultSubpartitionView subpartitionView;

	private volatile boolean isReleased;

	public LocalInputChannel(
		SingleInputGate inputGate,
		int channelIndex,
		ResultPartitionID partitionId,
		ResultPartitionManager partitionManager,
		TaskEventDispatcher taskEventDispatcher,
		TaskIOMetricGroup metrics) {

		this(inputGate, channelIndex, partitionId, partitionManager, taskEventDispatcher,
			0, 0, metrics);
	}

	public LocalInputChannel(
		SingleInputGate inputGate,
		int channelIndex,
		ResultPartitionID partitionId,
		ResultPartitionManager partitionManager,
		TaskEventDispatcher taskEventDispatcher,
		int initialBackoff,
		int maxBackoff,
		TaskIOMetricGroup metrics) {

		super(inputGate, channelIndex, partitionId, initialBackoff, maxBackoff, metrics.getNumBytesInLocalCounter());

		this.partitionManager = checkNotNull(partitionManager);
		this.taskEventDispatcher = checkNotNull(taskEventDispatcher);
	}

	// ------------------------------------------------------------------------
	// Consume
	// ------------------------------------------------------------------------

	/**
	 * 这个方法只会执行一次，请求当前inputChannel对应的 ResultSubPartition
	 * 在Task通过循环调用 InputGate.getNextBufferOrEvent 方法获取输入数据时，会请求一次(仅一次)
	 */
	//请求消费对应的子分区
	@Override
	void requestSubpartition(int subpartitionIndex) throws IOException, InterruptedException {

		boolean retriggerRequest = false;

		// The lock is required to request only once in the presence of retriggered requests.
		synchronized (requestLock) {
			checkState(!isReleased, "LocalInputChannel has been released already");

			if (subpartitionView == null) {
				LOG.debug("{}: Requesting LOCAL subpartition {} of partition {}.",
					this, subpartitionIndex, partitionId);

				try {
					//Local，无需网络通信，通过 ResultPartitionManager 创建一个 ResultSubpartitionView
					//LocalInputChannel 实现了 BufferAvailabilityListener
					//在有数据时会得到通知，notifyDataAvailable 会被调用，进而将当前 channel 加到 InputGate 的可用 Channel 队列中

					//奥，SubpartitionView竟然是在这里创建的，SubpartitionView就是用来消费 ResultSubPartition的数据的；
					ResultSubpartitionView subpartitionView = partitionManager.createSubpartitionView(
						partitionId, subpartitionIndex, this); //这里监听器设置的是this，所以当上游ResultPartition产生数据的时候，会调用 notifyDataAvailable() 方法

					if (subpartitionView == null) {
						throw new IOException("Error requesting subpartition.");
					}

					// make the subpartition view visible
					this.subpartitionView = subpartitionView;

					// check if the channel was released in the meantime
					if (isReleased) {
						subpartitionView.releaseAllResources();
						this.subpartitionView = null;
					}
				} catch (PartitionNotFoundException notFound) {
					if (increaseBackoff()) {
						retriggerRequest = true;
					} else {
						throw notFound;
					}
				}
			}
		}

		// Do this outside of the lock scope as this might lead to a
		// deadlock with a concurrent release of the channel via the
		// input gate.
		if (retriggerRequest) {
			inputGate.retriggerPartitionRequest(partitionId.getPartitionId());
		}
	}

	/**
	 * Retriggers a subpartition request.
	 */
	void retriggerSubpartitionRequest(Timer timer, final int subpartitionIndex) {
		synchronized (requestLock) {
			checkState(subpartitionView == null, "already requested partition");

			timer.schedule(new TimerTask() {
				@Override
				public void run() {
					try {
						requestSubpartition(subpartitionIndex);
					} catch (Throwable t) {
						setError(t);
					}
				}
			}, getCurrentBackoff());
		}
	}

	//读取数据，借助 ResultSubparitionView 消费 ResultSubPartition 中的数据
	@Override
	Optional<BufferAndAvailability> getNextBuffer() throws IOException, InterruptedException {
		checkError();

		ResultSubpartitionView subpartitionView = this.subpartitionView;
		if (subpartitionView == null) {
			// There is a possible race condition between writing a EndOfPartitionEvent (1) and flushing (3) the Local
			// channel on the sender side, and reading EndOfPartitionEvent (2) and processing flush notification (4). When
			// they happen in that order (1 - 2 - 3 - 4), flush notification can re-enqueue LocalInputChannel after (or
			// during) it was released during reading the EndOfPartitionEvent (2).
			if (isReleased) {
				return Optional.empty();
			}

			// this can happen if the request for the partition was triggered asynchronously
			// by the time trigger
			// would be good to avoid that, by guaranteeing that the requestPartition() and
			// getNextBuffer() always come from the same thread
			// we could do that by letting the timer insert a special "requesting channel" into the input gate's queue
			subpartitionView = checkAndWaitForSubpartitionView();
		}

		BufferAndBacklog next = subpartitionView.getNextBuffer(); //这里，直接通过subpartitionView来获取数据；

		if (next == null) {
			if (subpartitionView.isReleased()) {
				throw new CancelTaskException("Consumed partition " + subpartitionView + " has been released.");
			} else {
				return Optional.empty();
			}
		}

		numBytesIn.inc(next.buffer().getSizeUnsafe());
		return Optional.of(new BufferAndAvailability(next.buffer(), next.isMoreAvailable(), next.buffersInBacklog()));
	}

	/**
	 * 上游 ResultSubPartition 产生数据的时候，会回调这个方法
	 */
	@Override
	public void notifyDataAvailable() {
		notifyChannelNonEmpty();
		//然后这个方法会把当前这个channel(已经有数据了)加入到 InputGate的 inputChannelsWithData这个队列中，后续Task再从InputGate中消费的时候，就可以拿到数据了；
	}

	private ResultSubpartitionView checkAndWaitForSubpartitionView() {
		// synchronizing on the request lock means this blocks until the asynchronous request
		// for the partition view has been completed
		// by then the subpartition view is visible or the channel is released
		synchronized (requestLock) {
			checkState(!isReleased, "released");
			checkState(subpartitionView != null, "Queried for a buffer before requesting the subpartition.");
			return subpartitionView;
		}
	}

	// ------------------------------------------------------------------------
	// Task events
	// ------------------------------------------------------------------------

	@Override
	void sendTaskEvent(TaskEvent event) throws IOException {
		checkError();
		checkState(subpartitionView != null, "Tried to send task event to producer before requesting the subpartition.");

		//事件分发
		if (!taskEventDispatcher.publish(partitionId, event)) {
			throw new IOException("Error while publishing event " + event + " to producer. The producer could not be found.");
		}
	}

	// ------------------------------------------------------------------------
	// Life cycle
	// ------------------------------------------------------------------------

	@Override
	boolean isReleased() {
		return isReleased;
	}

	@Override
	void notifySubpartitionConsumed() throws IOException {
		if (subpartitionView != null) {
			subpartitionView.notifySubpartitionConsumed();
		}
	}

	/**
	 * Releases the partition reader
	 */
	@Override
	void releaseAllResources() throws IOException {
		if (!isReleased) {
			isReleased = true;

			ResultSubpartitionView view = subpartitionView;
			if (view != null) {
				view.releaseAllResources();
				subpartitionView = null;
			}
		}
	}

	@Override
	public String toString() {
		return "LocalInputChannel [" + partitionId + "]";
	}
}
