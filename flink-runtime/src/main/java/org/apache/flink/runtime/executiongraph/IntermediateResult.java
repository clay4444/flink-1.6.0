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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * 在 JobGraph 中用 IntermediateDataSet 表示 JobVertex 的对外输出，一个 JobGraph 可能有 n(n >=0) 个输出。在 ExecutionGraph 中，与此对应的就是 IntermediateResult。
 */
public class IntermediateResult {

	private final IntermediateDataSetID id;     //对应的IntermediateDataSet的ID

	private final ExecutionJobVertex producer; 	//生产者是上游的 ExecutionJobVertex


	/**
	 * 由于 ExecutionJobVertex 有 numParallelProducers 个并行的子任务，自然对应的每一个 IntermediateResult 就有 numParallelProducers 个生产者，
	 * 每个生产者的在相应的 IntermediateResult 上的输出对应一个 IntermediateResultPartition。IntermediateResultPartition 表示的是 ExecutionVertex 的一个输出分区，即：
	 *
	 * ExecutionJobVertex -->  IntermediateResult
	 * ExecutionVertex -->  IntermediateResultPartition
	 *
	 * IntermediateResultPartition 的生产者是ExecutionVertex，消费者是一个或若干个 ExecutionEdge。
	 */
	private final IntermediateResultPartition[] partitions;

	/**
	 * Maps intermediate result partition IDs to a partition index. This is
	 * used for ID lookups of intermediate results. I didn't dare to change the
	 * partition connect logic in other places that is tightly coupled to the
	 * partitions being held as an array.
	 */
	private final HashMap<IntermediateResultPartitionID, Integer> partitionLookupHelper = new HashMap<>();

	private final int numParallelProducers;   //对应ExecutionJobVertex的并行度

	private final AtomicInteger numberOfRunningProducers;

	private int partitionsAssigned;

	private int numConsumers;

	private final int connectionIndex;

	private final ResultPartitionType resultType;

	/**
	 * 根据 JobVertex 创建ExecutionJobVertex的时候会创建下游对应的 IntermediateResult，有几个IntermediateDataSetID就有几个 IntermediateResult
	 * @param id
	 * @param producer
	 * @param numParallelProducers
	 * @param resultType
	 */
	public IntermediateResult(
			IntermediateDataSetID id,
			ExecutionJobVertex producer,
			int numParallelProducers, 	//JobVertex的并行度，这个并行度决定着 IntermediateResultPartition 的个数，上游  ExecutionVertex 生产者的个数
			ResultPartitionType resultType) {

		this.id = checkNotNull(id);
		this.producer = checkNotNull(producer);

		checkArgument(numParallelProducers >= 1);
		this.numParallelProducers = numParallelProducers;

		this.partitions = new IntermediateResultPartition[numParallelProducers]; //partition 数组也创建了，但也只创建了数组，具体的partition是在创建ExecutionVertex的时候创建的；

		this.numberOfRunningProducers = new AtomicInteger(numParallelProducers);

		// we do not set the intermediate result partitions here, because we let them be initialized by
		// the execution vertex that produces them

		// assign a random connection index
		this.connectionIndex = (int) (Math.random() * Integer.MAX_VALUE);

		// The runtime type for this produced result
		this.resultType = checkNotNull(resultType);
	}

	public void setPartition(int partitionNumber, IntermediateResultPartition partition) {
		if (partition == null || partitionNumber < 0 || partitionNumber >= numParallelProducers) {
			throw new IllegalArgumentException();
		}

		if (partitions[partitionNumber] != null) {
			throw new IllegalStateException("Partition #" + partitionNumber + " has already been assigned.");
		}

		partitions[partitionNumber] = partition;
		partitionLookupHelper.put(partition.getPartitionId(), partitionNumber);
		partitionsAssigned++;
	}

	public IntermediateDataSetID getId() {
		return id;
	}

	public ExecutionJobVertex getProducer() {
		return producer;
	}

	public IntermediateResultPartition[] getPartitions() {
		return partitions;
	}

	/**
	 * Returns the partition with the given ID.
	 *
	 * @param resultPartitionId ID of the partition to look up
	 * @throws NullPointerException If partition ID <code>null</code>
	 * @throws IllegalArgumentException Thrown if unknown partition ID
	 * @return Intermediate result partition with the given ID
	 */
	public IntermediateResultPartition getPartitionById(IntermediateResultPartitionID resultPartitionId) {
		// Looks ups the partition number via the helper map and returns the
		// partition. Currently, this happens infrequently enough that we could
		// consider removing the map and scanning the partitions on every lookup.
		// The lookup (currently) only happen when the producer of an intermediate
		// result cannot be found via its registered execution.
		Integer partitionNumber = partitionLookupHelper.get(checkNotNull(resultPartitionId, "IntermediateResultPartitionID"));
		if (partitionNumber != null) {
			return partitions[partitionNumber];
		} else {
			throw new IllegalArgumentException("Unknown intermediate result partition ID " + resultPartitionId);
		}
	}

	public int getNumberOfAssignedPartitions() {
		return partitionsAssigned;
	}

	public ResultPartitionType getResultType() {
		return resultType;
	}

	public int registerConsumer() {
		final int index = numConsumers;
		numConsumers++;

		for (IntermediateResultPartition p : partitions) {
			if (p.addConsumerGroup() != index) {
				throw new RuntimeException("Inconsistent consumer mapping between intermediate result partitions.");
			}
		}
		return index;
	}

	public int getConnectionIndex() {
		return connectionIndex;
	}

	void resetForNewExecution() {
		this.numberOfRunningProducers.set(numParallelProducers);
	}

	int decrementNumberOfRunningProducersAndGetRemaining() {
		return numberOfRunningProducers.decrementAndGet();
	}

	boolean isConsumable() {
		if (resultType.isPipelined()) {
			return true;
		}
		else {
			return numberOfRunningProducers.get() == 0;
		}
	}

	@Override
	public String toString() {
		return "IntermediateResult " + id.toString();
	}
}
