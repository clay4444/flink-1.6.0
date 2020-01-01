/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.graph;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.operators.util.UserCodeObjectWrapper;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.InputFormatVertex;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.WithMasterCheckpointHook;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RescalePartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.tasks.StreamIterationHead;
import org.apache.flink.streaming.runtime.tasks.StreamIterationTail;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.SerializedValue;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * The StreamingJobGraphGenerator converts a {@link StreamGraph} into a {@link JobGraph}.
 */
@Internal
public class StreamingJobGraphGenerator {

	private static final Logger LOG = LoggerFactory.getLogger(StreamingJobGraphGenerator.class);

	// ------------------------------------------------------------------------
	//入口，先new出来，然后调用 createJobGraph() 方法
	public static JobGraph createJobGraph(StreamGraph streamGraph) {
		return new StreamingJobGraphGenerator(streamGraph).createJobGraph();
	}

	// ------------------------------------------------------------------------

	private final StreamGraph streamGraph;

	/**
	 * 在 StreamGraph 中，每一个算子（Operator） 对应了图中的一个节点（StreamNode）。
	 * StreamGraph 会被进一步优化，将多个符合条件的节点串联（Chain） 在一起形成一个节点，从而减少数据在不同节点之间流动所产生的序列化、反序列化、网络传输的开销。
	 * 多个算子被 chain 在一起的形成的节点在 JobGraph 中对应的就是 JobVertex。
	 */
	private final Map<Integer, JobVertex> jobVertices;   //< startStreamNode(chain的头节点) id，多个算子被chain在一起的形成的节点 >     startStreamNode(id) -> JobVertex 的对应关系

	private final JobGraph jobGraph;
	private final Collection<Integer> builtVertices;      //已经构建的JobVertex的id (只保存作为chain start 的 StreamNode的id)  集合

	private final List<StreamEdge> physicalEdgesInOrder;   //物理边集合（不包含chain内部的边）, 对于SocketTextStreamWordCount来说，只有两条边，4-5 和 2-4 ；按逆序排序

	private final Map<Integer, Map<Integer, StreamConfig>> chainedConfigs;   //保存 operator chain 的信息，部署时用来构建 OperatorChain，startNodeId -> (currentNodeId -> StreamConfig)

	private final Map<Integer, StreamConfig> vertexConfigs;   //所有每个 StreamNode 的配置信息，node id -> StreamConfig, 对于StreamConfig来说，每个StreamNode都要有对应的配置，这个配置是运行时需要用的；

	private final Map<Integer, String> chainedNames;       //保存每个 JobVertex 节点的名字，current Node id -> chainedName

	private final Map<Integer, ResourceSpec> chainedMinResources;
	private final Map<Integer, ResourceSpec> chainedPreferredResources;

	private final StreamGraphHasher defaultStreamGraphHasher;     //用于计算hash值的算法
	private final List<StreamGraphHasher> legacyStreamGraphHashers;

	private StreamingJobGraphGenerator(StreamGraph streamGraph) {
		this.streamGraph = streamGraph;
		this.defaultStreamGraphHasher = new StreamGraphHasherV2();
		this.legacyStreamGraphHashers = Arrays.asList(new StreamGraphUserHashHasher());

		this.jobVertices = new HashMap<>();
		this.builtVertices = new HashSet<>();
		this.chainedConfigs = new HashMap<>();
		this.vertexConfigs = new HashMap<>();
		this.chainedNames = new HashMap<>();
		this.chainedMinResources = new HashMap<>();
		this.chainedPreferredResources = new HashMap<>();
		this.physicalEdgesInOrder = new ArrayList<>();

		jobGraph = new JobGraph(streamGraph.getJobName());   //先把 JobGraph 直接new出来
	}

	//真正创建 JobGraph 的方法；
	private JobGraph createJobGraph() {

		// make sure that all vertices start immediately
		// 设置任务调度模式为所有节点均在一开始就启动(流处理系统任务调度一般都用整个模式)，批处理一般的任务调度模式为 LAZY_FROM_SOURCE
		jobGraph.setScheduleMode(ScheduleMode.EAGER);

		// Generate deterministic hashes for the nodes in order to identify them across
		// BFS 遍历 StreamGraph 并且为每个SteamNode生成hash，hash值将被用于 JobVertexId 中
		// 保证如果提交的拓扑没有改变，则每次生成的hash都是一样的，但是如果拓扑结构发生了改变，哪怕只是某个算子的并行度发生了变化，整个作业的所有operator的hash全部都会发生变化； @todo： 具体原理有时间再看吧
		//为所有节点生成一个唯一的hash id，如果节点在多次提交中没有改变（包括并发度、上下游等），那么这个hash id就不会改变，这主要用于故障恢复。这里我们不能用 StreamNode.id 来代替，因为这是一个从 1 开始的静态计数变量，同样的 Job 可能会得到不一样的 id。
		Map<Integer, byte[]> hashes = defaultStreamGraphHasher.traverseStreamGraphAndGenerateHashes(streamGraph);    //   < StreamNode id, 对应node生成的唯一hash值 >

		// Generate legacy version hashes for backwards compatibility
		// 为了保持兼容性创建的hash
		List<Map<Integer, byte[]>> legacyHashes = new ArrayList<>(legacyStreamGraphHashers.size());
		for (StreamGraphHasher hasher : legacyStreamGraphHashers) {
			legacyHashes.add(hasher.traverseStreamGraphAndGenerateHashes(streamGraph));
		}

		Map<Integer, List<Tuple2<byte[], byte[]>>> chainedOperatorHashes = new HashMap<>();  //< NodeId, operate byte[] >


		//生成jobvertex，JobEdge，串成chain等
		//这里的逻辑大致可以理解为，挨个遍历节点，如果该节点是一个chain的头节点，
		// 就生成一个JobVertex，如果不是头节点，就要把自身配置并入头节点，然后把头节点和自己的出边相连；
		// 对于不能chain的节点，当作只有头节点处理即可
		setChaining(hashes, legacyHashes, chainedOperatorHashes);

		// 将每个JobVertex的输入边集合也序列化到该JobVertex的StreamConfig中
		// (物理出边集合已经在setChaining的时候写入了对应 headOperator(StreamNode)对应的config中了，在311行config.setOutEdgesInOrder；注意只有chain的headOperator有物理出边)
		setPhysicalEdges();

		// 根据group name，为每个 JobVertex 指定所属的 SlotSharingGroup
		// 以及针对 Iteration的头尾设置  CoLocationGroup
		setSlotSharingAndCoLocation();

		//配置检查点
		configureCheckpointing();

		// 添加用户提供的自定义的文件信息
		JobGraphGenerator.addUserArtifactEntries(streamGraph.getEnvironment().getCachedFiles(), jobGraph);

		// 将 StreamGraph 的 ExecutionConfig 序列化到 JobGraph 的配置中
		try {
			jobGraph.setExecutionConfig(streamGraph.getExecutionConfig());
		}
		catch (IOException e) {
			throw new IllegalConfigurationException("Could not serialize the ExecutionConfig." +
					"This indicates that non-serializable types (like custom serializers) were registered");
		}

		return jobGraph;
	}

	/**
	 * 为每个 headOperator 设置入边 (第一个headOperator不用设置)
	 * 并序列化进对应的StreamConfig；
	 */
	private void setPhysicalEdges() {
		Map<Integer, List<StreamEdge>> physicalInEdgesInOrder = new HashMap<Integer, List<StreamEdge>>();   //每个StreamNode id(target) 对应的入边；

		for (StreamEdge edge : physicalEdgesInOrder) {   //physicalEdgesInOrder：物理出边集合
			int target = edge.getTargetId();

			List<StreamEdge> inEdges = physicalInEdgesInOrder.get(target); //找target对应的入边

			// create if not set
			if (inEdges == null) {  //没找到就设置；
				inEdges = new ArrayList<>();
				physicalInEdgesInOrder.put(target, inEdges);
			}

			inEdges.add(edge); //填充每个target对应的入边；
		}

		for (Map.Entry<Integer, List<StreamEdge>> inEdges : physicalInEdgesInOrder.entrySet()) {
			int vertex = inEdges.getKey();  //Node id
			List<StreamEdge> edgeList = inEdges.getValue(); //入边

			vertexConfigs.get(vertex).setInPhysicalEdges(edgeList);  //为这个Node对应的 StreamConfig 设置它对应的输入边(本质上也就是为JobVertex设置输入边)；  此时每个JobVertex对应的 物理 输入边和输出边就都构建好了；
		}
	}

	/**
	 * Sets up task chains from the source {@link StreamNode} instances.
	 *
	 * 从 source StreamNode 开始设置 task chain
	 * 递归的创建所有的 JobVertex 实例；
	 *
	 * <p>This will recursively create all {@link JobVertex} instances.
	 * hashes < StreamNode id, 之前生成的hash值  >
	 *
	 */
	private void setChaining(Map<Integer, byte[]> hashes, List<Map<Integer, byte[]>> legacyHashes, Map<Integer, List<Tuple2<byte[], byte[]>>> chainedOperatorHashes) {
		for (Integer sourceNodeId : streamGraph.getSourceIDs()) {
			createChain(sourceNodeId, sourceNodeId, hashes, legacyHashes, 0, chainedOperatorHashes);
		}
	}

	/**
	 * 过程实际上就是通过 DFS 遍历所有的 StreamNode, 并按照 chainable 的条件不停地将可以串联的 operator 放在同一个的 operator chain 中。
	 * 每一个 StreamNode 的配置信息都会被序列化到对应的 StreamConfig 中。只有 operator chain 的头部节点会生成对应的 JobVertex ，
	 * 一个 operator chain 的所有内部节点都会以序列化的形式写入头部节点的 CHAINED_TASK_CONFIG 配置项中。
	 */

	//构建 operator chain（可能包含一个或多个StreamNode），返回值是当前的这个 operator chain 实际的输出边（不包括内部的边）
	//如果 currentNodeId != startNodeId, 说明当前节点在  operator chain 的内部
	private List<StreamEdge> createChain(
			Integer startNodeId,     //开始的Nodeid，
			Integer currentNodeId,   //当前的Nodeid， 开始时等于 startNodeId；
			Map<Integer, byte[]> hashes,
			List<Map<Integer, byte[]>> legacyHashes,
			int chainIndex,
			Map<Integer, List<Tuple2<byte[], byte[]>>> chainedOperatorHashes) {

		if (!builtVertices.contains(startNodeId)) {  //首先确保没有构建过 以这个 start Node 为头节点的 JobVertex

			//当前 operator chain 最终的输出边，不包括内部的边
			List<StreamEdge> transitiveOutEdges = new ArrayList<StreamEdge>();

			List<StreamEdge> chainableOutputs = new ArrayList<StreamEdge>();      //可以和当前 node chain 在一起的输出边，
			List<StreamEdge> nonChainableOutputs = new ArrayList<StreamEdge>();   //不能和当前 node chain 在一起的边

			//将当前节点的出边分为两组，即 chainable 和 nonChainable
			for (StreamEdge outEdge : streamGraph.getStreamNode(currentNodeId).getOutEdges()) { //找出当前节点连接的后续Node，
				if (isChainable(outEdge, streamGraph)) {  //这条边连接的两个节点(source节点是当前节点，sink节点是sink节点) 可以chain到一起；
					chainableOutputs.add(outEdge);
				} else {
					nonChainableOutputs.add(outEdge);
				}
			}

			//对于chainable的输出边，递归调用，找到最终的输出边并加入到输出列表中
			for (StreamEdge chainable : chainableOutputs) {
				transitiveOutEdges.addAll(
						createChain(startNodeId, chainable.getTargetId(), hashes, legacyHashes, chainIndex + 1, chainedOperatorHashes)); //递归的继续往下找；
			}

			//对于 nonChainable 的边
			for (StreamEdge nonChainable : nonChainableOutputs) {
				//这个边本身就应该加入到当前节点的输出列表中
				transitiveOutEdges.add(nonChainable);

				//递归调用，以下游节点为起点创建新的operator chain
				createChain(nonChainable.getTargetId(), nonChainable.getTargetId(), hashes, legacyHashes, 0, chainedOperatorHashes);
			}

			//用于保存一个operator chain所有 operator 的 hash 信息
			List<Tuple2<byte[], byte[]>> operatorHashes =
				chainedOperatorHashes.computeIfAbsent(startNodeId, k -> new ArrayList<>());

			byte[] primaryHashBytes = hashes.get(currentNodeId);

			for (Map<Integer, byte[]> legacyHash : legacyHashes) {
				operatorHashes.add(new Tuple2<>(primaryHashBytes, legacyHash.get(currentNodeId)));
			}

			//设置当前节点的名称，资源要求等信息，走到这里，说明后续的递归过程已经走完并返回了；
			chainedNames.put(currentNodeId, createChainedName(currentNodeId, chainableOutputs));
			chainedMinResources.put(currentNodeId, createChainedMinResources(currentNodeId, chainableOutputs));
			chainedPreferredResources.put(currentNodeId, createChainedPreferredResources(currentNodeId, chainableOutputs));

			//如果当前节点是起始节点, 则直接创建 JobVertex 并返回 StreamConfig, 否则先创建一个空的 StreamConfig
			//createJobVertex 函数就是根据 StreamNode 创建对应的 JobVertex, 并返回了空的 StreamConfig
			StreamConfig config = currentNodeId.equals(startNodeId)
					? createJobVertex(startNodeId, hashes, legacyHashes, chainedOperatorHashes)  //从这里也可以看到，如果currentNodeId != startNodeId，不会创建JobVertex；
					: new StreamConfig(new Configuration());

			// 设置 JobVertex 的 StreamConfig, 基本上是序列化 StreamNode 中的配置到 StreamConfig 中.
			// 其中包括 序列化器, StreamOperator, Checkpoint 等相关配置
			setVertexConfig(currentNodeId, config, chainableOutputs, nonChainableOutputs);

			if (currentNodeId.equals(startNodeId)) { //说明这个StreamNode要作为头节点封装为一个JobVertex

				// 如果是chain的起始节点。（不是chain中的节点，也会被标记成 chain start）
				config.setChainStart();
				config.setChainIndex(0);
				config.setOperatorName(streamGraph.getStreamNode(currentNodeId).getOperatorName());
				//把实际的输出边写入配置, 部署时会用到
				config.setOutEdgesInOrder(transitiveOutEdges); //最后的节点没有物理输出边
				//operator chain 的头部 operator 的输出边，包括内部的边
				config.setOutEdges(streamGraph.getStreamNode(currentNodeId).getOutEdges());

				// 将当前节点(headOfChain)与所有出边相连
				for (StreamEdge edge : transitiveOutEdges) {
					// 通过StreamEdge构建出JobEdge，创建IntermediateDataSet，用来将JobVertex和JobEdge相连
					connect(startNodeId, edge);  //DAG的连接关系主要通过这个方法来维护；
				}

				// 将operator chain中所有子节点的 StreamConfig 写入到 headOfChain 节点的 CHAINED_TASK_CONFIG 配置中
				config.setTransitiveChainedTaskConfigs(chainedConfigs.get(startNodeId));

			} else {  //currentNodeId 要和 startNodeId chain在一起；封装为一个JobVertex

				//如果是 operator chain 内部的节点
				Map<Integer, StreamConfig> chainedConfs = chainedConfigs.get(startNodeId);

				if (chainedConfs == null) {
					chainedConfigs.put(startNodeId, new HashMap<Integer, StreamConfig>());
				}
				config.setChainIndex(chainIndex);  //这个config是当前节点的config
				StreamNode node = streamGraph.getStreamNode(currentNodeId); 	//currentNode
				config.setOperatorName(node.getOperatorName());
				// 将当前节点的 StreamConfig 添加到所在的 operator chain 的 config 集合中
				chainedConfigs.get(startNodeId).put(currentNodeId, config);
			}

			//设置当前 operator 的 OperatorID
			config.setOperatorID(new OperatorID(primaryHashBytes));

			if (chainableOutputs.isEmpty()) {
				config.setChainEnd(); //chain结束
			}
			return transitiveOutEdges;

		} else {
			return new ArrayList<>();
		}
	}

	/**
	 * 为当前的chain 链 task，设置name
	 * @param vertexID    current Node id
	 * @param chainedOutputs  chain之后的的输出边(内部的不算)
	 * @return
	 */
	private String createChainedName(Integer vertexID, List<StreamEdge> chainedOutputs) {
		String operatorName = streamGraph.getStreamNode(vertexID).getOperatorName();
		if (chainedOutputs.size() > 1) {
			List<String> outputChainedNames = new ArrayList<>();
			for (StreamEdge chainable : chainedOutputs) {
				outputChainedNames.add(chainedNames.get(chainable.getTargetId()));
			}
			return operatorName + " -> (" + StringUtils.join(outputChainedNames, ", ") + ")";
		} else if (chainedOutputs.size() == 1) {
			return operatorName + " -> " + chainedNames.get(chainedOutputs.get(0).getTargetId());
		} else {
			return operatorName;  //后续没有边，直接返回这个currentNode 的name
		}
	}

	private ResourceSpec createChainedMinResources(Integer vertexID, List<StreamEdge> chainedOutputs) {
		ResourceSpec minResources = streamGraph.getStreamNode(vertexID).getMinResources();
		for (StreamEdge chainable : chainedOutputs) {
			minResources = minResources.merge(chainedMinResources.get(chainable.getTargetId()));
		}
		return minResources;
	}

	private ResourceSpec createChainedPreferredResources(Integer vertexID, List<StreamEdge> chainedOutputs) {
		ResourceSpec preferredResources = streamGraph.getStreamNode(vertexID).getPreferredResources();
		for (StreamEdge chainable : chainedOutputs) {
			preferredResources = preferredResources.merge(chainedPreferredResources.get(chainable.getTargetId()));
		}
		return preferredResources;
	}

	/**
	 * 创建 JobVertex
	 * @param streamNodeId   startNodeId
	 * @param hashes        <StreamNodeid, 随机生成的hash >
	 * @param legacyHashes
	 * @param chainedOperatorHashes
	 * @return
	 */
	private StreamConfig createJobVertex(
			Integer streamNodeId,
			Map<Integer, byte[]> hashes,
			List<Map<Integer, byte[]>> legacyHashes,
			Map<Integer, List<Tuple2<byte[], byte[]>>> chainedOperatorHashes) {

		JobVertex jobVertex;
		StreamNode streamNode = streamGraph.getStreamNode(streamNodeId);

		byte[] hash = hashes.get(streamNodeId);

		if (hash == null) {
			throw new IllegalStateException("Cannot find node hash. " +
					"Did you generate them before calling this method?");
		}

		JobVertexID jobVertexId = new JobVertexID(hash);

		List<JobVertexID> legacyJobVertexIds = new ArrayList<>(legacyHashes.size());
		for (Map<Integer, byte[]> legacyHash : legacyHashes) {
			hash = legacyHash.get(streamNodeId);
			if (null != hash) {
				legacyJobVertexIds.add(new JobVertexID(hash));
			}
		}

		List<Tuple2<byte[], byte[]>> chainedOperators = chainedOperatorHashes.get(streamNodeId);
		List<OperatorID> chainedOperatorVertexIds = new ArrayList<>();
		List<OperatorID> userDefinedChainedOperatorVertexIds = new ArrayList<>();
		if (chainedOperators != null) {
			for (Tuple2<byte[], byte[]> chainedOperator : chainedOperators) {
				chainedOperatorVertexIds.add(new OperatorID(chainedOperator.f0));
				userDefinedChainedOperatorVertexIds.add(chainedOperator.f1 != null ? new OperatorID(chainedOperator.f1) : null);
			}
		}

		if (streamNode.getInputFormat() != null) {
			jobVertex = new InputFormatVertex(
					chainedNames.get(streamNodeId),
					jobVertexId,
					legacyJobVertexIds,
					chainedOperatorVertexIds,
					userDefinedChainedOperatorVertexIds);
			TaskConfig taskConfig = new TaskConfig(jobVertex.getConfiguration());
			taskConfig.setStubWrapper(new UserCodeObjectWrapper<Object>(streamNode.getInputFormat()));
		} else {
			jobVertex = new JobVertex(     //创建JobVertex，
					chainedNames.get(streamNodeId),  //chained name
					jobVertexId,
					legacyJobVertexIds,
					chainedOperatorVertexIds,
					userDefinedChainedOperatorVertexIds);
		}

		jobVertex.setResources(chainedMinResources.get(streamNodeId), chainedPreferredResources.get(streamNodeId));

		jobVertex.setInvokableClass(streamNode.getJobVertexClass());   //streamNode中保存的tm能运行的具体Task类型赋给 JobVertex

		int parallelism = streamNode.getParallelism();

		if (parallelism > 0) {
			jobVertex.setParallelism(parallelism);
		} else {
			parallelism = jobVertex.getParallelism();
		}

		jobVertex.setMaxParallelism(streamNode.getMaxParallelism());

		if (LOG.isDebugEnabled()) {
			LOG.debug("Parallelism set: {} for {}", parallelism, streamNodeId);
		}

		jobVertices.put(streamNodeId, jobVertex);
		builtVertices.add(streamNodeId);
		jobGraph.addVertex(jobVertex); //为 jobGraph 添加 jobVertex，jobGraph 中维护dag只需要所有的 jobVertex；

		return new StreamConfig(jobVertex.getConfiguration());  //关键点：StreamConfig的config保存的就是 jobVertex 的Configuration，所以往StreamConfig赋值就是给对应的jobVertex 的Configuration赋值；
	}

	/**
	 * 设置 JobVertex 的 StreamConfig, 基本上是序列化 StreamNode 中的配置到 StreamConfig 中.
	 * 其中包括 序列化器, StreamOperator, Checkpoint 等相关配置
	 * @param vertexID  currentNode id
	 * @param config   空的config，主要就是把StreamNode的信息序列化到这个config中；
	 * @param chainableOutputs
	 * @param nonChainableOutputs
	 */
	@SuppressWarnings("unchecked")
	private void setVertexConfig(Integer vertexID, StreamConfig config,
			List<StreamEdge> chainableOutputs, List<StreamEdge> nonChainableOutputs) {

		StreamNode vertex = streamGraph.getStreamNode(vertexID); //获取 StreamNode

		config.setVertexID(vertexID);
		config.setBufferTimeout(vertex.getBufferTimeout());

		config.setTypeSerializerIn1(vertex.getTypeSerializerIn1());
		config.setTypeSerializerIn2(vertex.getTypeSerializerIn2());
		config.setTypeSerializerOut(vertex.getTypeSerializerOut());

		// iterate edges, find sideOutput edges create and save serializers for each outputTag type
		for (StreamEdge edge : chainableOutputs) {
			if (edge.getOutputTag() != null) {
				config.setTypeSerializerSideOut(
					edge.getOutputTag(),
					edge.getOutputTag().getTypeInfo().createSerializer(streamGraph.getExecutionConfig())
				);
			}
		}
		for (StreamEdge edge : nonChainableOutputs) {
			if (edge.getOutputTag() != null) {
				config.setTypeSerializerSideOut(
						edge.getOutputTag(),
						edge.getOutputTag().getTypeInfo().createSerializer(streamGraph.getExecutionConfig())
				);
			}
		}

		config.setStreamOperator(vertex.getOperator());  //设置 operator (封装用户代码)
		config.setOutputSelectors(vertex.getOutputSelectors());

		config.setNumberOfOutputs(nonChainableOutputs.size());
		config.setNonChainedOutputs(nonChainableOutputs);
		config.setChainedOutputs(chainableOutputs);

		config.setTimeCharacteristic(streamGraph.getEnvironment().getStreamTimeCharacteristic()); // 事件时间还是处理时间

		final CheckpointConfig ceckpointCfg = streamGraph.getCheckpointConfig();   //checkpoint 配置

		config.setStateBackend(streamGraph.getStateBackend());    //backend
		config.setCheckpointingEnabled(ceckpointCfg.isCheckpointingEnabled());
		if (ceckpointCfg.isCheckpointingEnabled()) {
			config.setCheckpointMode(ceckpointCfg.getCheckpointingMode());
		}
		else {
			// the "at-least-once" input handler is slightly cheaper (in the absence of checkpoints),
			// so we use that one if checkpointing is not enabled
			config.setCheckpointMode(CheckpointingMode.AT_LEAST_ONCE);
		}
		config.setStatePartitioner(0, vertex.getStatePartitioner1());
		config.setStatePartitioner(1, vertex.getStatePartitioner2());
		config.setStateKeySerializer(vertex.getStateKeySerializer());

		Class<? extends AbstractInvokable> vertexClass = vertex.getJobVertexClass();   //tm运行的实际的task类型， SourceStreamTask / OneInputStreamTask / TwoInputStreamTask ....

		if (vertexClass.equals(StreamIterationHead.class)
				|| vertexClass.equals(StreamIterationTail.class)) {
			config.setIterationId(streamGraph.getBrokerID(vertexID));
			config.setIterationWaitTime(streamGraph.getLoopTimeout(vertexID));
		}

		List<StreamEdge> allOutputs = new ArrayList<StreamEdge>(chainableOutputs);
		allOutputs.addAll(nonChainableOutputs);

		vertexConfigs.put(vertexID, config);
	}

	/**
	 * 通过StreamEdge构建出JobEdge，创建IntermediateDataSet，用来将JobVertex和JobEdge相连
	 * @param headOfChain  startNode id
	 * @param edge         StreamEdge
	 */
	private void connect(Integer headOfChain, StreamEdge edge) {

		physicalEdgesInOrder.add(edge); //物理边(区别于chain内部的边)，是从后往前加的，也就是逆序的；

		Integer downStreamvertexID = edge.getTargetId();  //targetNode id

		//上下游节点
		JobVertex headVertex = jobVertices.get(headOfChain);
		JobVertex downStreamVertex = jobVertices.get(downStreamvertexID);

		StreamConfig downStreamConfig = new StreamConfig(downStreamVertex.getConfiguration());

		downStreamConfig.setNumberOfInputs(downStreamConfig.getNumberOfInputs() + 1);  //下游JobVertex的 input + 1

		StreamPartitioner<?> partitioner = edge.getPartitioner();
		JobEdge jobEdge;
		if (partitioner instanceof ForwardPartitioner || partitioner instanceof RescalePartitioner) {
			jobEdge = downStreamVertex.connectNewDataSetAsInput(
				headVertex,
				DistributionPattern.POINTWISE,
				ResultPartitionType.PIPELINED_BOUNDED);
		} else {
			jobEdge = downStreamVertex.connectNewDataSetAsInput(  //创建 JobEdge，创建IntermediateDataSet，并连接起来；
					headVertex,
					DistributionPattern.ALL_TO_ALL,
					ResultPartitionType.PIPELINED_BOUNDED);
		}
		// set strategy name so that web interface can show it.
		jobEdge.setShipStrategyName(partitioner.toString());

		if (LOG.isDebugEnabled()) {
			LOG.debug("CONNECTED: {} - {} -> {}", partitioner.getClass().getSimpleName(),
					headOfChain, downStreamvertexID);
		}
	}

	/**
	 * @param edge   这个边连接的两个节点是否能chain到一起
	 */
	public static boolean isChainable(StreamEdge edge, StreamGraph streamGraph) {
		StreamNode upStreamVertex = edge.getSourceVertex();
		StreamNode downStreamVertex = edge.getTargetVertex();

		StreamOperator<?> headOperator = upStreamVertex.getOperator();
		StreamOperator<?> outOperator = downStreamVertex.getOperator();

		return downStreamVertex.getInEdges().size() == 1     //下游节点只有一个输入
				&& outOperator != null
				&& headOperator != null
				&& upStreamVertex.isSameSlotSharingGroup(downStreamVertex)   		//在同一个 slot 共享组中
				&& outOperator.getChainingStrategy() == ChainingStrategy.ALWAYS  	//上下游算子的 chainning 策略，要允许 chainning
				&& (headOperator.getChainingStrategy() == ChainingStrategy.HEAD ||
					headOperator.getChainingStrategy() == ChainingStrategy.ALWAYS)
				&& (edge.getPartitioner() instanceof ForwardPartitioner)            //上下游算子之间的数据传输方式必须是FORWARD，而不能是REBALANCE等其它模式
				&& upStreamVertex.getParallelism() == downStreamVertex.getParallelism() //上下游算子的并行度要一致
				&& streamGraph.isChainingEnabled();  								// StreamExecutionEnvironment 配置允许 chainning
	}

	private void setSlotSharingAndCoLocation() {
		final HashMap<String, SlotSharingGroup> slotSharingGroups = new HashMap<>();
		final HashMap<String, Tuple2<SlotSharingGroup, CoLocationGroup>> coLocationGroups = new HashMap<>();

		for (Entry<Integer, JobVertex> entry : jobVertices.entrySet()) {

			final StreamNode node = streamGraph.getStreamNode(entry.getKey());
			final JobVertex vertex = entry.getValue();

			// configure slot sharing group
			final String slotSharingGroupKey = node.getSlotSharingGroup();
			final SlotSharingGroup sharingGroup;

			if (slotSharingGroupKey != null) {
				sharingGroup = slotSharingGroups.computeIfAbsent(
						slotSharingGroupKey, (k) -> new SlotSharingGroup());
				vertex.setSlotSharingGroup(sharingGroup);
			} else {
				sharingGroup = null;
			}

			// configure co-location constraint
			final String coLocationGroupKey = node.getCoLocationGroup();
			if (coLocationGroupKey != null) {
				if (sharingGroup == null) {
					throw new IllegalStateException("Cannot use a co-location constraint without a slot sharing group");
				}

				Tuple2<SlotSharingGroup, CoLocationGroup> constraint = coLocationGroups.computeIfAbsent(
						coLocationGroupKey, (k) -> new Tuple2<>(sharingGroup, new CoLocationGroup()));

				if (constraint.f0 != sharingGroup) {
					throw new IllegalStateException("Cannot co-locate operators from different slot sharing groups");
				}

				vertex.updateCoLocationGroup(constraint.f1);
			}
		}

		for (Tuple2<StreamNode, StreamNode> pair : streamGraph.getIterationSourceSinkPairs()) {

			CoLocationGroup ccg = new CoLocationGroup();

			JobVertex source = jobVertices.get(pair.f0.getId());
			JobVertex sink = jobVertices.get(pair.f1.getId());

			ccg.addVertex(source);
			ccg.addVertex(sink);
			source.updateCoLocationGroup(ccg);
			sink.updateCoLocationGroup(ccg);
		}

	}

	/**
	 * 配置checkpoint信息 (在jobGraph创建的时候执行：createJobGraph方法)，主要包含三个容器
	 *
	 * triggerVertices：只包含那些作为source的节点
	 * ackVertices：    包含所有的节点
	 * commitVertices： 包含所有的节点
	 */
	private void configureCheckpointing() {
		CheckpointConfig cfg = streamGraph.getCheckpointConfig();

		long interval = cfg.getCheckpointInterval();
		if (interval > 0) {
			ExecutionConfig executionConfig = streamGraph.getExecutionConfig();
			// propagate the expected behaviour for checkpoint errors to task.
			executionConfig.setFailTaskOnCheckpointError(cfg.isFailOnCheckpointingErrors());
		} else {
			// interval of max value means disable periodic checkpoint
			interval = Long.MAX_VALUE;
		}

		//  --- configure the participating vertices ---

		// collect the vertices that receive "trigger checkpoint" messages.
		// currently, these are all the sources
		List<JobVertexID> triggerVertices = new ArrayList<>();

		// collect the vertices that need to acknowledge the checkpoint
		// currently, these are all vertices
		List<JobVertexID> ackVertices = new ArrayList<>(jobVertices.size());

		// collect the vertices that receive "commit checkpoint" messages
		// currently, these are all vertices
		List<JobVertexID> commitVertices = new ArrayList<>(jobVertices.size());

		for (JobVertex vertex : jobVertices.values()) {
			if (vertex.isInputVertex()) {
				triggerVertices.add(vertex.getID());  // <<<< 在这里可以看出ExecutionGraphBuilder
			}
			commitVertices.add(vertex.getID());
			ackVertices.add(vertex.getID());
		}

		//  --- configure options ---

		CheckpointRetentionPolicy retentionAfterTermination;
		if (cfg.isExternalizedCheckpointsEnabled()) {
			CheckpointConfig.ExternalizedCheckpointCleanup cleanup = cfg.getExternalizedCheckpointCleanup();
			// Sanity check
			if (cleanup == null) {
				throw new IllegalStateException("Externalized checkpoints enabled, but no cleanup mode configured.");
			}
			retentionAfterTermination = cleanup.deleteOnCancellation() ?
					CheckpointRetentionPolicy.RETAIN_ON_FAILURE :
					CheckpointRetentionPolicy.RETAIN_ON_CANCELLATION;
		} else {
			retentionAfterTermination = CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION;
		}

		CheckpointingMode mode = cfg.getCheckpointingMode();

		boolean isExactlyOnce;
		if (mode == CheckpointingMode.EXACTLY_ONCE) {
			isExactlyOnce = true;
		} else if (mode == CheckpointingMode.AT_LEAST_ONCE) {
			isExactlyOnce = false;
		} else {
			throw new IllegalStateException("Unexpected checkpointing mode. " +
				"Did not expect there to be another checkpointing mode besides " +
				"exactly-once or at-least-once.");
		}

		//  --- configure the master-side checkpoint hooks ---

		final ArrayList<MasterTriggerRestoreHook.Factory> hooks = new ArrayList<>();

		for (StreamNode node : streamGraph.getStreamNodes()) {
			StreamOperator<?> op = node.getOperator();
			if (op instanceof AbstractUdfStreamOperator) {
				Function f = ((AbstractUdfStreamOperator<?, ?>) op).getUserFunction();

				if (f instanceof WithMasterCheckpointHook) {
					hooks.add(new FunctionMasterCheckpointHookFactory((WithMasterCheckpointHook<?>) f));
				}
			}
		}

		// because the hooks can have user-defined code, they need to be stored as
		// eagerly serialized values
		final SerializedValue<MasterTriggerRestoreHook.Factory[]> serializedHooks;
		if (hooks.isEmpty()) {
			serializedHooks = null;
		} else {
			try {
				MasterTriggerRestoreHook.Factory[] asArray =
						hooks.toArray(new MasterTriggerRestoreHook.Factory[hooks.size()]);
				serializedHooks = new SerializedValue<>(asArray);
			}
			catch (IOException e) {
				throw new FlinkRuntimeException("Trigger/restore hook is not serializable", e);
			}
		}

		// because the state backend can have user-defined code, it needs to be stored as
		// eagerly serialized value
		final SerializedValue<StateBackend> serializedStateBackend;
		if (streamGraph.getStateBackend() == null) {
			serializedStateBackend = null;
		} else {
			try {
				serializedStateBackend =
					new SerializedValue<StateBackend>(streamGraph.getStateBackend());
			}
			catch (IOException e) {
				throw new FlinkRuntimeException("State backend is not serializable", e);
			}
		}

		//  --- done, put it all together ---

		JobCheckpointingSettings settings = new JobCheckpointingSettings(
			triggerVertices,
			ackVertices,
			commitVertices,
			new CheckpointCoordinatorConfiguration(
				interval,
				cfg.getCheckpointTimeout(),
				cfg.getMinPauseBetweenCheckpoints(),
				cfg.getMaxConcurrentCheckpoints(),
				retentionAfterTermination,
				isExactlyOnce),
			serializedStateBackend,
			serializedHooks);

		jobGraph.setSnapshotSettings(settings);
	}
}
