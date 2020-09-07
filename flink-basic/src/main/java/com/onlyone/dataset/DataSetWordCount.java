package com.onlyone.dataset;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @Author onlyone
 */
public class DataSetWordCount {

    public static void main(String[] args) throws Exception {

        // 创建Flink运行的上下文环境
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 使用 fromElements 函数创建一个 DataSet 对象
        // 创建DataSet，这里我们的输入是一行一行的文本
        DataSet<String> text = env.fromElements(
                "Fk Spark Storm",
                "Flink Flink Flink",
                "Spark Spark Spark",
                "Storm Storm Storm");
        // 通过Flink内置的转换函数进行计算
        DataSet<Tuple2<String, Integer>> counts =
                text.flatMap(new LineSplitter())
                        .groupBy(0)
                        .sum(1);
        //结果打印
        counts.printToErr();
    }

    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // 将文本分割
            String[] tokens = value.toLowerCase().split("\\W+");
            Arrays.stream(tokens).filter(t -> t.length() > 0).forEach(t -> out.collect(new Tuple2<String, Integer>(t, 1)));

        }
    }

}

