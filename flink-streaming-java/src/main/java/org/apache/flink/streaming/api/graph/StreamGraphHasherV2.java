/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.graph;

import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.transformations.StreamTransformation;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;

import org.apache.flink.shaded.guava18.com.google.common.hash.HashFunction;
import org.apache.flink.shaded.guava18.com.google.common.hash.Hasher;
import org.apache.flink.shaded.guava18.com.google.common.hash.Hashing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import static org.apache.flink.util.StringUtils.byteToHexString;

/**
 * StreamGraphHasher from Flink 1.2. This contains duplicated code to ensure that the algorithm does not change with
 * future Flink versions.
 *
 * <p>DO NOT MODIFY THIS CLASS
 */
public class StreamGraphHasherV2 implements StreamGraphHasher {

	private static final Logger LOG = LoggerFactory.getLogger(StreamGraphHasherV2.class);

	/**
	 * Returns a map with a hash for each {@link StreamNode} of the {@link
	 * StreamGraph}. The hash is used as the {@link JobVertexID} in order to
	 * identify nodes across job submissions if they didn't change.
	 *
	 * <p>The complete {@link StreamGraph} is traversed. The hash is either
	 * computed from the transformation's user-specified id (see
	 * {@link StreamTransformation#getUid()}) or generated in a deterministic way.
	 *
	 * <p>The generated hash is deterministic with respect to:
	 * <ul>
	 *   <li>node-local properties (like parallelism, UDF, node ID),
	 *   <li>chained output nodes, and
	 *   <li>input nodes hashes
	 * </ul>
	 *
	 * @return A map from {@link StreamNode#id} to hash as 16-byte array.
	 */

	/**
	 * 为 StreamGraph 中的每个 StreamNode 生成一个hash值，这个哈希值会被用作 JobVertexID，保证如果提交的拓扑没有改变，则每次生成的hash都是一样的
	 */
	@Override
	public Map<Integer, byte[]> traverseStreamGraphAndGenerateHashes(StreamGraph streamGraph) {
		// The hash function used to generate the hash
		final HashFunction hashFunction = Hashing.murmur3_128(0);
		final Map<Integer, byte[]> hashes = new HashMap<>();   //res   < NodeId, Node的哈希值 >

		Set<Integer> visited = new HashSet<>();
		Queue<StreamNode> remaining = new ArrayDeque<>();  //BFS 使用队列

		// We need to make the source order deterministic. The source IDs are
		// not returned in the same order, which means that submitting the same
		// program twice might result in different traversal, which breaks the
		// deterministic hash assignment.
		List<Integer> sources = new ArrayList<>();    //先找出所有source
		for (Integer sourceNodeId : streamGraph.getSourceIDs()) {
			sources.add(sourceNodeId);
		}
		Collections.sort(sources);

		//
		// Traverse the graph in a breadth-first manner. Keep in mind that
		// the graph is not a tree and multiple paths to nodes can exist.
		//

		// Start with source nodes
		for (Integer sourceNodeId : sources) {
			remaining.add(streamGraph.getStreamNode(sourceNodeId));  // source 作为 start
			visited.add(sourceNodeId);
		}

		StreamNode currentNode;
		while ((currentNode = remaining.poll()) != null) {
			// Generate the hash code. Because multiple path exist to each
			// node, we might not have all required inputs available to
			// generate the hash code.
			if (generateNodeHash(currentNode, hashFunction, hashes, streamGraph.isChainingEnabled())) {  //生成哈希值；
				// Add the child nodes
				for (StreamEdge outEdge : currentNode.getOutEdges()) {
					StreamNode child = outEdge.getTargetVertex();  //找所有后继节点，

					if (!visited.contains(child.getId())) {  //如果没有遍历过，就遍历处理；
						remaining.add(child);
						visited.add(child.getId());
					}
				}
			} else {
				// We will revisit this later.
				visited.remove(currentNode.getId());
			}
		}

		return hashes;
	}

	/**
	 * 返回是否生成成功
	 *
	 * Generates a hash for the node and returns whether the operation was
	 * successful.
	 *
	 * @param node         The node to generate the hash for
	 * @param hashFunction The hash function to use
	 * @param hashes       The current state of generated hashes
	 * @return <code>true</code> if the node hash has been generated.
	 * <code>false</code>, otherwise. If the operation is not successful, the
	 * hash needs be generated at a later point when all input is available.
	 * @throws IllegalStateException If node has user-specified hash and is
	 *                               intermediate node of a chain
	 */
	private boolean generateNodeHash(
			StreamNode node,  //为哪个 StreamNode 生成哈希值
			HashFunction hashFunction,
			Map<Integer, byte[]> hashes,
			boolean isChainingEnabled) {

		// Check for user-specified ID
		String userSpecifiedHash = node.getTransformationUID();

		if (userSpecifiedHash == null) {
			// Check that all input nodes have their hashes computed
			for (StreamEdge inEdge : node.getInEdges()) {
				// If the input node has not been visited yet, the current
				// node will be visited again at a later point when all input
				// nodes have been visited and their hashes set.

				//必须从前往后依次处理，前面的处理完了才能处理后面的；
				if (!hashes.containsKey(inEdge.getSourceId())) {
					return false;
				}
			}

			Hasher hasher = hashFunction.newHasher();
			byte[] hash = generateDeterministicHash(node, hasher, hashes, isChainingEnabled); //为node生成一个明确的hash值；

			if (hashes.put(node.getId(), hash) != null) {
				// Sanity check
				throw new IllegalStateException("Unexpected state. Tried to add node hash " +
						"twice. This is probably a bug in the JobGraph generator.");
			}

			return true;
		} else {
			Hasher hasher = hashFunction.newHasher();
			byte[] hash = generateUserSpecifiedHash(node, hasher);

			for (byte[] previousHash : hashes.values()) {
				if (Arrays.equals(previousHash, hash)) {
					throw new IllegalArgumentException("Hash collision on user-specified ID. " +
							"Most likely cause is a non-unique ID. Please check that all IDs " +
							"specified via `uid(String)` are unique.");
				}
			}

			if (hashes.put(node.getId(), hash) != null) {
				// Sanity check
				throw new IllegalStateException("Unexpected state. Tried to add node hash " +
						"twice. This is probably a bug in the JobGraph generator.");
			}

			return true;
		}
	}

	/**
	 * Generates a hash from a user-specified ID.
	 */
	private byte[] generateUserSpecifiedHash(StreamNode node, Hasher hasher) {
		hasher.putString(node.getTransformationUID(), Charset.forName("UTF-8"));

		return hasher.hash().asBytes();
	}

	/**
	 * Generates a deterministic hash from node-local properties and input and
	 * output edges.
	 *
	 * 根据 node 的属性，input edge，output edge，生成一个确定的hash值；
	 * 也就是根据拓扑结构，生成一个确定的hash值，这样在恢复作业的时候，如果拓扑结构没变，就可以根据hash id值找到对应的state 恢复
	 */
	private byte[] generateDeterministicHash(
			StreamNode node,
			Hasher hasher,
			Map<Integer, byte[]> hashes,
			boolean isChainingEnabled) {

		// Include stream node to hash. We use the current size of the computed
		// hashes as the ID. We cannot use the node's ID, because it is
		// assigned from a static counter. This will result in two identical
		// programs having different hashes.
		//使用当前的 size 大小 作为 hasher 的id，而不能使用node id，因为node id是根据一个静态的counter生成的，它会导致相同的拓扑结构程序生成不同的 hash
		generateNodeLocalHash(node, hasher, hashes.size());

		// Include chained nodes to hash
		for (StreamEdge outEdge : node.getOutEdges()) {   //找当前节点的出边，看是否能chain在一起
			if (isChainable(outEdge, isChainingEnabled)) {
				StreamNode chainedNode = outEdge.getTargetVertex();

				// Use the hash size again, because the nodes are chained to
				// this node. This does not add a hash for the chained nodes.
				//hash size不用+1，因为后续的这个node是chain到当前node中的，这两个Node在同一个 JobVertex 中；
				generateNodeLocalHash(chainedNode, hasher, hashes.size());
			}
		}

		byte[] hash = hasher.hash().asBytes();

		// Make sure that all input nodes have their hash set before entering
		// this loop (calling this method).
		for (StreamEdge inEdge : node.getInEdges()) {
			byte[] otherHash = hashes.get(inEdge.getSourceId());

			// Sanity check
			if (otherHash == null) {
				throw new IllegalStateException("Missing hash for input node "
						+ inEdge.getSourceVertex() + ". Cannot generate hash for "
						+ node + ".");
			}

			for (int j = 0; j < hash.length; j++) {
				hash[j] = (byte) (hash[j] * 37 ^ otherHash[j]);
			}
		}

		if (LOG.isDebugEnabled()) {
			String udfClassName = "";
			if (node.getOperator() instanceof AbstractUdfStreamOperator) {
				udfClassName = ((AbstractUdfStreamOperator<?, ?>) node.getOperator())
						.getUserFunction().getClass().getName();
			}

			LOG.debug("Generated hash '" + byteToHexString(hash) + "' for node " +
					"'" + node.toString() + "' {id: " + node.getId() + ", " +
					"parallelism: " + node.getParallelism() + ", " +
					"user function: " + udfClassName + "}");
		}

		return hash;
	}

	/**
	 * Applies the {@link Hasher} to the {@link StreamNode} (only node local
	 * attributes are taken into account). The hasher encapsulates the current
	 * state of the hash.
	 *
	 * <p>The specified ID is local to this node. We cannot use the
	 * {@link StreamNode#id}, because it is incremented in a static counter.
	 * Therefore, the IDs for identical jobs will otherwise be different.
	 */
	private void generateNodeLocalHash(StreamNode node, Hasher hasher, int id) {
		// This resolves conflicts for otherwise identical source nodes. BUT
		// the generated hash codes depend on the ordering of the nodes in the
		// stream graph.
		hasher.putInt(id);
	}

	/**
	 * StreamEdge 两端的节点是否能够被 chain 到同一个 JobVertex 中
	 * @return  true / false
	 */
	private boolean isChainable(StreamEdge edge, boolean isChainingEnabled) {

		//获取到上游和下游节点
		StreamNode upStreamVertex = edge.getSourceVertex();
		StreamNode downStreamVertex = edge.getTargetVertex();

		//获取到上游和下游节点具体的算子 StreamOperator
		StreamOperator<?> headOperator = upStreamVertex.getOperator();
		StreamOperator<?> outOperator = downStreamVertex.getOperator();

		return downStreamVertex.getInEdges().size() == 1    //下游节点只有一个输入
				&& outOperator != null
				&& headOperator != null
				&& upStreamVertex.isSameSlotSharingGroup(downStreamVertex)    //在同一个slot共享组中

				//上下游算子的 chainning 策略，要允许chainning
				//默认的是 ALWAYS
				//在添加算子时，也可以强制使用 disableChain 设置为 NEVER
				&& outOperator.getChainingStrategy() == ChainingStrategy.ALWAYS
				&& (headOperator.getChainingStrategy() == ChainingStrategy.HEAD ||
				headOperator.getChainingStrategy() == ChainingStrategy.ALWAYS)

			    //上下游节点之间的数据传输方式必须是FORWARD，而不能是REBALANCE等其它模式
				&& (edge.getPartitioner() instanceof ForwardPartitioner)
				&& upStreamVertex.getParallelism() == downStreamVertex.getParallelism()  //上下游节点的并行度要一致
				&& isChainingEnabled;
	}
}
