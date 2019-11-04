package org.apache.flink.test.SourceCodeRead;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * TransFormations 逻辑内容
 *      source -> flatmap -> partition -> keyArr -> print
 * id      1	    2			3   		4		  5
 *
 * TransFormations 具体内容
 *      flatmap -> keyArr -> print
 * id     2			4   	   5
 *
 * 生成 StreamGraph 的过程：
 *
 * 先处理 flatmap，然后递归处理 source，把source作为StreamNode 加入 StreamGraph(但是没有加边)，StreamNode中有：
 *    StreamOperator(封装用户代码)，tm执行的具体任务类型(这里是SourceStreamTask，此外还有OneInputStreamTask,TwoInputStreamTask 等) 等
 *
 * 然后返回处理 flatmap (还返回了source input ids)，开始时和上面过程一样，封装为StreamNode 加入 StreamGraph，不同的比如StreamNode中的tm执行的任务类型为OneInputStreamTask,
 * 最大的不同是调用 transformOneInputTransform 的时候会有加边的操作，而上面的 transformSource 是没有的，
 * 往 StreamGraph 加边的时候要注意的就是 逻辑节点的合并，select union partition 等
 * 加边的过程就是使用 source 返回的 source input ids 和 当前 flatmap 算子的id，构造出边的集合，然后加入 StreamGraph 中；
 *
 * 然后处理 keyArr，还是调用transformOneInputTransform，然后根据input，递归处理 PartitionTransformation ，transformPartition方法，然后递归的找partition算子的input，也就是flatmap，可以直接找到并返回input ids，
 * 然后为这个partition算子生成一个新的虚拟id(6)，把这个虚拟id加入 StreamGraph 的 virtualPartitionNodes，返回新建的虚拟id 给 keyArr 作为input ids
 * 注意并没有为这个 PartitionTransformation 构建StreamNode
 *
 * 递归返回后继续处理 keyArr，先构建StreamNode，放进StreamGraph，然后构建边，构建的过程中会发现上游节点是虚拟id节点6，然后会找出真正的上游节点，也就是2，然后在这两个物理 StreamNode之间构建 StreamEdge；
 * 注意：在构建 StreamEdge 过程中，还会把虚拟的partition节点的 partitioner 融合到 StreamEdge中； 也就是把 select split partiton 等逻辑节点合并到 StreamEdge 上；
 *
 * 最后处理print算子，调用 transformSink() 方法，递归处理上游keyArr，直接返回 input ids(4)， 然后为StreamGraph建立StreamNode 和 sink，然后建立最后一条Edge
 *
 * 整个过程结束
 *
 *
 * 生成 JobGraph的过程：
 *
 * 首先逻辑结构大概是类似这样的
 * JobVertex  ->IntermediateDataSet -> JobEdge  -> JobVertex -> IntermediateDataSet -> JobEdge -> JobVertex
 *
 * 不同于StreamGraph中每个算子对应一个StreamNode；JobGraph 中将多个符合条件的节点串联（Chain） 在一起形成一个节点，叫 JobVertex，JobVertex中保存着所有的所有的输入 JobEdge，输出 IntermediateDataSet
 * JobEdge 上游连接的是 IntermediateDataSet 中间结果变量，下游连接的是 JobVertex
 * IntermediateDataSet 有三种中间数据类型，blocking   pipline   piplined_bounded， 上游连接JobVertex，下游连接JobEdge；JobVertex是生产者，JobEdge是消费者；
 *
 * 开始创建JobGraph，先用BFS方式遍历StreamGraph，尝试为每个StreamNode生成一个明确的hash值，拓扑结构不变，则生成的hash值不变，在生成哈希值的过程中，会尝试把后面的Node和当前的Node合并在一起(具体策略看isChainable()方法代码)，
 * 首先source和flatmap会合并在一起；flatmap和keyAgg不会合并在一起，因为keyAgg的chain策略是 HEAD； keyAgg和sink不会合并在一起，因为他们之间的edge的数据分区方式是 rebalance，不是forward； 最后还剩一个sink算子
 * 但是这里好像也没有真正合并啊。。。。。。只是验证了一下？
 *
 * 然后开始调用setChaining()方法，生成 JobVertex / JobEdge 等，
 * 从source开始往下走，startNodeId是1，currentNodeId是1， 1和2可以chain，所以把边1-2加入chainableOutputs，递归，startNodeId是1，currentNodeId是2，2和4无法chain，把边2-4加入nonChainableOutputs，继续递归
 * startNodeId是4，currentNodeId是4，4和5不能chain，把边4-5加入nonChainableOutputs，继续递归5-5，startNodeId是5，currentNodeId是5，此时没有后续的边，所以开始createJobVertex，transitiveOutEdges为空，不用为jobEdge前后关系建立链接；第一个JobVertex建立完成；
 * 递归结束回到 (4,4)，过程和 (5,5)类似，但是transitiveOutEdges有一条边4-5，所以会和上一个JobVertex通过JobEdge建立链接关系；递归结束回到 (1,2)，此时startNodeId和currentNodeId不相等，此时没有建立createJobVertex的过程，也没有链接connect的过程，仅仅是缓存了一下(chainedConfigs)，
 * 递归结束回到 (1,1)，createJobVertex构建JobVertex，设置StreamConfig，transitiveOutEdges有一条边2-4（递归过程(1,2)的返回结果，因为startNodeId是1，currentNodeId是2后续无法再chain，所有返回物理边 2-4 ），根据这条边connect维护dag链接关系，建立和上一个JobVertex的连接关系(使用JobEdge和IntermediateDataSet)
 * 并把(1,2)过程中产生的streamNode id为2的算子的 StreamConfig (chainedConfigs中)序列化到 headOfChain 节点的 CHAINED_TASK_CONFIG 配置中，此时Stream 1和2 的 StreamConfig 都添加到了这一个 JobVertex 中；
 * 整个递归过程结束，生成了三个JobVertex，连接关系也使用 JobEdge 和 IntermediateDataSet 建立好了，
 * 需要注意的是 StreamConfig 是对应StreamNode的，每个StreamNode都需要生成config，只是有些 StreamNode 会合并为一个 JobVertex，此时后续StreamNode的 StreamConfig就会序列化到 chainNode 的config的chainedTaskConfig_属性中；
 *
 * 总结一下：
 * 整个过程实际上就是通过 DFS 遍历所有的 StreamNode, 并按照 chainable 的条件不停地将可以串联的 operator 放在同一个的 operator chain 中。
 * 每一个 StreamNode 的配置信息都会被序列化到对应的 StreamConfig 中。但是只有 operator chain 的头部节点会生成对应的 JobVertex ，而一个 operator chain 的所有内部节点都会以序列化的形式写入头部节点的 CHAINED_TASK_CONFIG 配置项中。
 *
 * JobGraph 的关键在于将多个 StreamNode 优化为一个 JobVertex, 对应的 StreamEdge 则转化为 JobEdge, 并且 JobVertex 和 JobEdge 之间通过 IntermediateDataSet 形成一个生产者和消费者的连接关系。
 *
 */
public class SocketTextStreamWordCount {
    public static void main(String[] args) throws Exception {
        /*if (args.length != 2) {
            System.err.println("USAGE:\nSocketTextStreamWordCount <hostname> <p ort>");
            return;
        }*/
        String hostName = "127.0.0.1";
        Integer port = 9999;

        // set up the execution environment
        /**
         * 这行代码会返回一个可用的执行环境。执行环境是整个flink程序执行的上下文，记录了相关配置（如并行度等），
         * 并提供了一系列方法，如读取输入流的方法，以及真正开始运行整个代码的 execute 方法等。
         */
        //final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		//设置一个 restport
		Configuration configuration = new Configuration();
		configuration.setInteger(RestOptions.PORT, 9898);
		final StreamExecutionEnvironment env = LocalStreamEnvironment.createLocalEnvironment(Runtime.getRuntime().availableProcessors(),configuration);

        // get input data
        DataStream<String> text = env.socketTextStream(hostName, port);

        /*text.map(new MapFunction<String, String>() {
			@Override
			public String map(String value) {
				return value;
			}
		}).print();*/

        text.flatMap(new LineSplitter()).setParallelism(1) // group by the tuple field "0" and sum up tuple field "1"
                .keyBy(1).sum(1).setParallelism(1).print();

		System.out.println(env.getExecutionPlan());

        // execute program
        env.execute("Java WordCount from SocketTextStream Example");
    }

    /**
     * Implements the string tokenizer that splits sentences into words as a user-defined
     * <p>
     * FlatMapFunction. The function takes a line (String) and splits it in to
     * <p>
     * multiple pairs in the form of "(word,1)" (Tuple2&lt;String, Integer& gt;).
     */

    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override

        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) { // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");
            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }
    }
}
