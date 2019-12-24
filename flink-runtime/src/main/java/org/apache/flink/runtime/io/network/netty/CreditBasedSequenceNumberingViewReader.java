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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.NetworkSequenceViewReader;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.partition.consumer.LocalInputChannel;

import java.io.IOException;

/**
 * Simple wrapper for the subpartition view used in the new network credit-based mode.
 *
 * <p>It also keeps track of available buffers and notifies the outbound
 * handler about non-emptiness, similar to the {@link LocalInputChannel}.
 * <p>
 * 这个类相当于对 ResultSubpartitionView 的一层包装，她会按顺序为读取的每一个 buffer 分配一个序列号，并且记录了接收数据的 RemoteInputChannel 的 ID。
 * 它实现了 BufferAvailabilityListener 接口，因而可以作为 PipelinedSubpartitionView 的回调对象 (也就是说当上游的ResultSubpartitionView有数据产生时，会通知这个类)
 */
class CreditBasedSequenceNumberingViewReader implements BufferAvailabilityListener, NetworkSequenceViewReader {

	private final Object requestLock = new Object();

	private final InputChannelID receiverId;

	//要赋给的另外一个 channel handler
	private final PartitionRequestQueue requestQueue;

	//消费 ResultSubpartition 的数据，并在 ResultSubpartition 有数据可用时获得通知
	private volatile ResultSubpartitionView subpartitionView;

	/**
	 * The status indicating whether this reader is already enqueued in the pipeline for transferring
	 * data or not.
	 *
	 * <p>It is mainly used to avoid repeated registrations but should be accessed by a single
	 * thread only since there is no synchronisation.
	 */
	private boolean isRegisteredAsAvailable = false;

	/**
	 * The number of available buffers for holding data on the consumer side.
	 */
	//numCreditsAvailable的值是消费端还能够容纳的buffer的数量，也就是允许生产端发送的buffer的数量
	private int numCreditsAvailable;

	private int sequenceNumber = -1;

	//构造器
	CreditBasedSequenceNumberingViewReader(
		InputChannelID receiverId,
		int initialCredit,
		PartitionRequestQueue requestQueue) {

		this.receiverId = receiverId;
		this.numCreditsAvailable = initialCredit;
		this.requestQueue = requestQueue;
	}

	//NettyServer接收到client的PartitionRequest请求之后，调用这个方法，来连接这个请求要消费的 ResultSubPartitionView，
	//这个是我们之前熟悉的，ResultSubPartitionView是用来消费对应的ResultSubPartition的数据的，
	@Override
	public void requestSubpartitionView(
		ResultPartitionProvider partitionProvider,
		ResultPartitionID resultPartitionId,
		int subPartitionIndex) throws IOException {

		synchronized (requestLock) {
			if (subpartitionView == null) {
				// This this call can trigger a notification we have to
				// schedule a separate task at the event loop that will
				// start consuming this. Otherwise the reference to the
				// view cannot be available in getNextBuffer().
				this.subpartitionView = partitionProvider.createSubpartitionView(  //其实最终还是调用的 ResultPartitionManager#createSubpartitionView
					resultPartitionId,
					subPartitionIndex,
					this);//自身充当监听器，当连接的SubpartitionView有数据写入时，会回调 notifyDataAvailable() 方法
			} else {
				throw new IllegalStateException("Subpartition already requested");
			}
		}
	}

	//加信用，代表客户端可以消费的数据变多了；
	@Override
	public void addCredit(int creditDeltas) {
		numCreditsAvailable += creditDeltas;  //可用信用增加
	}

	@Override
	public void setRegisteredAsAvailable(boolean isRegisteredAvailable) {
		this.isRegisteredAsAvailable = isRegisteredAvailable;
	}

	@Override
	public boolean isRegisteredAsAvailable() {
		return isRegisteredAsAvailable;
	}

	/**
	 * Returns true only if the next buffer is an event or the reader has both available
	 * credits and buffers.
	 */

	//是否还可以消费数据：
	// 1. ResultSubpartition 中有更多的数据
	// 2. credit > 0 或者下一条数据是事件(事件不需要消耗credit)
	@Override
	public boolean isAvailable() {
		// BEWARE: this must be in sync with #isAvailable(BufferAndBacklog)!
		return hasBuffersAvailable() &&
			//要求 numCreditsAvailable > 0 或者是 Event
			(numCreditsAvailable > 0 || subpartitionView.nextBufferIsEvent());
	}

	/**
	 * Check whether this reader is available or not (internal use, in sync with
	 * {@link #isAvailable()}, but slightly faster).
	 *
	 * <p>Returns true only if the next buffer is an event or the reader has both available
	 * credits and buffers.
	 *
	 * @param bufferAndBacklog current buffer and backlog including information about the next buffer
	 */

	//和上面 isAvailable() 是等价的
	private boolean isAvailable(BufferAndBacklog bufferAndBacklog) {
		// BEWARE: this must be in sync with #isAvailable()!
		return bufferAndBacklog.isMoreAvailable() &&
			(numCreditsAvailable > 0 || bufferAndBacklog.nextBufferIsEvent());
	}

	@Override
	public InputChannelID getReceiverId() {
		return receiverId;
	}

	@Override
	public int getSequenceNumber() {
		return sequenceNumber;
	}

	@VisibleForTesting
	int getNumCreditsAvailable() {
		return numCreditsAvailable;
	}

	@VisibleForTesting
	boolean hasBuffersAvailable() {
		return subpartitionView.isAvailable();
	}


	//读取数据
	@Override
	public BufferAndAvailability getNextBuffer() throws IOException, InterruptedException {
		BufferAndBacklog next = subpartitionView.getNextBuffer();  //直接从 subpartitionView 中读取数据
		if (next != null) {
			sequenceNumber++;  //序列号

			//要发送一个buffer，对应的 numCreditsAvailable 要减 1
			if (next.buffer().isBuffer() && --numCreditsAvailable < 0) {  // >>>>>>>>>>>> 整个算法的核心在这里可以看到，当没有信用值了，即使上游 ResultSubPartitionView 中有数据buffer，也不会往下游发；
				throw new IllegalStateException("no credit available");
			}

			return new BufferAndAvailability(
				next.buffer(), isAvailable(next), next.buffersInBacklog());
		} else {
			return null;
		}
	}

	@Override
	public void notifySubpartitionConsumed() throws IOException {
		subpartitionView.notifySubpartitionConsumed();
	}

	@Override
	public boolean isReleased() {
		return subpartitionView.isReleased();
	}

	@Override
	public Throwable getFailureCause() {
		return subpartitionView.getFailureCause();
	}

	@Override
	public void releaseAllResources() throws IOException {
		subpartitionView.releaseAllResources();
	}

	/**
	 * 当这个类连接的上游 ResultSubPartitionView 开始产生数据的时候，会回调这个方法，
	 */
	@Override
	public void notifyDataAvailable() {
		requestQueue.notifyReaderNonEmpty(this);   //然后通知另外一个channel handler，这个reader对应的 ResultSubPartitionView 开始产生数据了；
	}

	@Override
	public String toString() {
		return "CreditBasedSequenceNumberingViewReader{" +
			"requestLock=" + requestLock +
			", receiverId=" + receiverId +
			", sequenceNumber=" + sequenceNumber +
			", numCreditsAvailable=" + numCreditsAvailable +
			", isRegisteredAsAvailable=" + isRegisteredAsAvailable +
			'}';
	}
}
