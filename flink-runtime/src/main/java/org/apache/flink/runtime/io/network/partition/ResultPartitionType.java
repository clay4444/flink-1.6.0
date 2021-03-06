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

/**
 * 最好结合 Flink 的内存管理机制来看；
 */
public enum ResultPartitionType {


	//blocking 落盘？
	BLOCKING(false, false, false),

	//pipelined  直接传给下游 ?
	PIPELINED(true, true, false),

	/**
	 * Pipelined partitions with a bounded (local) buffer pool.
	 *
	 * For streaming jobs, a fixed limit on the buffer pool size should help avoid that too much
	 * data is being buffered and checkpoint barriers are delayed. In contrast to limiting the
	 * overall network buffer pool size, this, however, still allows to be flexible with regards
	 * to the total number of partitions by selecting an appropriately big network buffer pool size.
	 *
	 * For batch jobs, it will be best to keep this unlimited ({@link #PIPELINED}) since there are
	 * no checkpoint barriers.
	 *
	 * 使用一个本地有界的缓冲池作为 Pipelined partitions
	 * 对于流作业来说，一个固定大小限制的缓冲池，有助于避免大量数据被缓存，checkpoint barrier 被延迟的情况；
	 *
	 * 目前在 Stream 模式下使用的类型是 PIPELINED_BOUNDED(true, true, true)，三个属性都是 true。
	 */
	PIPELINED_BOUNDED(true, true, true);

	/** Can the partition be consumed while being produced? */
	private final boolean isPipelined;

	/** Does the partition produce back pressure when not consumed? */
	private final boolean hasBackPressure;

	/** Does this partition use a limited number of (network) buffers? */
	private final boolean isBounded;

	/**
	 * Specifies the behaviour of an intermediate result partition at runtime.
	 */
	ResultPartitionType(boolean isPipelined, boolean hasBackPressure, boolean isBounded) {
		this.isPipelined = isPipelined;
		this.hasBackPressure = hasBackPressure;
		this.isBounded = isBounded;
	}

	public boolean hasBackPressure() {
		return hasBackPressure;
	}

	public boolean isBlocking() {
		return !isPipelined;
	}

	public boolean isPipelined() {
		return isPipelined;
	}

	/**
	 * Whether this partition uses a limited number of (network) buffers or not.
	 *
	 * @return <tt>true</tt> if the number of buffers should be bound to some limit
	 */
	public boolean isBounded() {
		return isBounded;
	}
}
