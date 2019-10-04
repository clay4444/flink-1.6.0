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
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

import java.util.ArrayList;
import java.util.List;

/**
 * 每个 ExecutionVertex 对应 IntermediateResult 中的一个 IntermediateResultPartition
 */
public class IntermediateResultPartition {

	private final IntermediateResult totalResult;  //当前partiton属于哪个IntermediateResult

	private final ExecutionVertex producer;  	   //上游承接哪个 ExecutionVertex 作为 生产者

	private final int partitionNumber;		      //subtask index 作为 partitionNumber(相当于partiton id)

	private final IntermediateResultPartitionID partitionId;

	private List<List<ExecutionEdge>> consumers;  //下游的 ExecutionEdge s

	/**
	 * 创建 ExecutionVertex 的时候，创建下游对应 IntermediateResult 中的一个 IntermediateResultPartition
	 * @param totalResult   //当前partiton属于哪个IntermediateResult
	 * @param producer		//上游承接哪个 ExecutionVertex 作为 生产者
	 * @param partitionNumber  //subtask index 作为 partitionNumber(相当于partiton id)
	 */
	public IntermediateResultPartition(IntermediateResult totalResult, ExecutionVertex producer, int partitionNumber) {
		this.totalResult = totalResult;
		this.producer = producer;
		this.partitionNumber = partitionNumber;
		this.consumers = new ArrayList<List<ExecutionEdge>>(0); //初始化下游的 ExecutionEdge s
		this.partitionId = new IntermediateResultPartitionID(); //随机指定一个id
	}

	public ExecutionVertex getProducer() {
		return producer;
	}

	public int getPartitionNumber() {
		return partitionNumber;
	}

	public IntermediateResult getIntermediateResult() {
		return totalResult;
	}

	public IntermediateResultPartitionID getPartitionId() {
		return partitionId;
	}

	ResultPartitionType getResultType() {
		return totalResult.getResultType();
	}

	public List<List<ExecutionEdge>> getConsumers() {
		return consumers;
	}

	public boolean isConsumable() {
		return totalResult.isConsumable();
	}

	int addConsumerGroup() {
		int pos = consumers.size();

		// NOTE: currently we support only one consumer per result!!!
		if (pos != 0) {
			throw new RuntimeException("Currently, each intermediate result can only have one consumer.");
		}

		consumers.add(new ArrayList<ExecutionEdge>());
		return pos;
	}

	void addConsumer(ExecutionEdge edge, int consumerNumber) {
		consumers.get(consumerNumber).add(edge);
	}

	boolean markFinished() {
		// Sanity check that this is only called on blocking partitions.
		if (!getResultType().isBlocking()) {
			throw new IllegalStateException("Tried to mark a non-blocking result partition as finished");
		}

		final int refCnt = totalResult.decrementNumberOfRunningProducersAndGetRemaining();

		if (refCnt == 0) {
			return true;
		}
		else if (refCnt  < 0) {
			throw new IllegalStateException("Decremented number of unfinished producers below 0. "
					+ "This is most likely a bug in the execution state/intermediate result "
					+ "partition management.");
		}

		return false;
	}
}
