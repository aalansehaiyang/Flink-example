package com.onlyone.DataStreamAPI04;


import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class MyStreamingSource implements SourceFunction<MyStreamingSource.Item> {

    private boolean isRunning = true;

    /**
     * 重写run方法产生一个源源不断的数据发送源
     *
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext<Item> ctx) throws Exception {
        while (isRunning) {
            Item item = generateItem();
            ctx.collect(item);

            //每秒产生一条数据
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    //随机产生一条商品数据
    private Item generateItem() {
        int i = new Random().nextInt(100);

        Item item = new Item();
        item.setName("name" + i);
        item.setId(i);
        return item;
    }

    class Item {
        private String name;
        private Integer id;

        Item() {
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Integer getId() {
            return id;
        }

        public void setId(Integer id) {
            this.id = id;
        }

        @Override
        public String toString() {
            return "Item{" +
                    "name='" + name + '\'' +
                    ", id=" + id +
                    '}';
        }
    }
}


class StreamingDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取数据源
        DataStreamSource<MyStreamingSource.Item> items = env.addSource(new MyStreamingSource()).setParallelism(1);


        //Map
        SingleOutputStreamOperator<Object> mapItems = items.map(item -> item.getName());
        // 自定义自己的 Map 函数。通过重写 MapFunction 或 RichMapFunction 来自定义自己的 map 函数。
        //SingleOutputStreamOperator<String> mapItems = items.map(new MyMapFunction());


        // FlatMap 接受一个元素，返回零到多个元素。FlatMap 和 Map 有些类似，
        // 但是当返回值是列表的时候，FlatMap 会将列表“平铺”，也就是以单个元素的形式进行输出。
//        SingleOutputStreamOperator<Object> flatMapItems = items.flatMap(new FlatMapFunction<MyStreamingSource.Item, Object>() {
//            @Override
//            public void flatMap(MyStreamingSource.Item item, Collector<Object> collector) throws Exception {
//                String name = item.getName();
//                collector.collect(name);
//            }
//        });

        // Fliter 的意思就是过滤掉不需要的数据，每个元素都会被 filter 函数处理，如果 filter 函数返回 true 则保留，否则丢弃。
//        SingleOutputStreamOperator<MyStreamingSource.Item> filterItems = items.filter(item -> item.getId() % 2 == 0);

        //打印结果
//        filterItems.print().setParallelism(1);
        mapItems.print().setParallelism(1);
        String jobName = "user defined streaming source";
        env.execute(jobName);
    }

    static class MyMapFunction extends RichMapFunction<MyStreamingSource.Item, String> {

        @Override
        public String map(MyStreamingSource.Item item) throws Exception {
            return item.getName();
        }
    }

}
