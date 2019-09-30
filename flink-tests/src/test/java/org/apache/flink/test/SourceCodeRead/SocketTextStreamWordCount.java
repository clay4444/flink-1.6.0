package org.apache.flink.test.SourceCodeRead;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

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
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // get input data
        DataStream<String> text = env.socketTextStream(hostName, port);

        /*text.map(new MapFunction<String, String>() {
			@Override
			public String map(String value) {
				return value;
			}
		});*/

        text.flatMap(new LineSplitter()).setParallelism(1) // group by the tuple field "0" and sum up tuple field "1"
                .keyBy(0).sum(1).setParallelism(1).print();

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
