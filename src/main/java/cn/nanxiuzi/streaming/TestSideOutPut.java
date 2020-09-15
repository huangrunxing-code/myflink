package cn.nanxiuzi.streaming;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.List;

/**
 * 主要学习旁路分流器的功能,适用于分流场景
 * 官方地址https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/stream/side_output.html
 * 一般分流有三种
 * Filter：分一次流就需要遍历一次原始流，太浪费时间和资源
 * Split：不能进行二次分流
 * SideOutPut：官方最为推荐的分流方法，最新提供的可以进行多次分流，无需担心爆出异常
 */
public class TestSideOutPut {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取数据源
        List data = new ArrayList<Tuple3<Integer,Integer,Integer>>();
        data.add(new Tuple3<>(0,1,0));
        data.add(new Tuple3<>(0,1,1));
        data.add(new Tuple3<>(0,2,2));
        data.add(new Tuple3<>(0,1,3));
        data.add(new Tuple3<>(1,2,5));
        data.add(new Tuple3<>(1,2,9));
        data.add(new Tuple3<>(1,2,11));
        data.add(new Tuple3<>(1,2,13));
        DataStreamSource ds = env.fromCollection(data);
        OutputTag<Tuple3<Integer,Integer,Integer>> out0 = new OutputTag<Tuple3<Integer,Integer,Integer>>("out0"){};
        OutputTag<Tuple3<Integer,Integer,Integer>> out1 = new OutputTag<Tuple3<Integer,Integer,Integer>>("out1"){};

        SingleOutputStreamOperator process = ds.process(new ProcessFunction<Tuple3<Integer, Integer, Integer>, Tuple3<Integer, Integer, Integer>>() {
            @Override
            public void processElement(Tuple3<Integer, Integer, Integer> value, Context ctx, Collector<Tuple3<Integer, Integer, Integer>> out) throws Exception {


                out.collect(Tuple3.of(value.f0,value.f1,101));
                if (value.f0 == 0) {
                    ctx.output(out0, value);
                } else {
                    ctx.output(out1, value);
                }
            }
        });
        process.print();
        process.getSideOutput(out0).printToErr();
        process.getSideOutput(out1).print();

        env.execute("");


    }

}
class TestFilter{
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取数据源
        List data = new ArrayList<Tuple3<Integer,Integer,Integer>>();
        data.add(new Tuple3<>(0,1,0));
        data.add(new Tuple3<>(0,1,1));
        data.add(new Tuple3<>(0,2,2));
        data.add(new Tuple3<>(0,1,3));
        data.add(new Tuple3<>(1,2,5));
        data.add(new Tuple3<>(1,2,9));
        data.add(new Tuple3<>(1,2,11));
        data.add(new Tuple3<>(1,2,13));
        DataStreamSource ds = env.fromCollection(data);
        SingleOutputStreamOperator filter1 = ds.filter((FilterFunction<Tuple3<Integer, Integer, Integer>>) value -> value.f0 == 0);
        SingleOutputStreamOperator filter2 = ds.filter((FilterFunction<Tuple3<Integer, Integer, Integer>>) value -> value.f0 == 1);
        filter1.print();
        filter2.printToErr();
        env.execute("");
    }
}

 class TestSplit{
     public static void main(String[] args) throws Exception {
         StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
         //获取数据源
         List data = new ArrayList<Tuple3<Integer,Integer,Integer>>();
         data.add(new Tuple3<>(0,1,0));
         data.add(new Tuple3<>(0,1,1));
         data.add(new Tuple3<>(0,2,2));
         data.add(new Tuple3<>(0,1,3));
         data.add(new Tuple3<>(1,2,5));
         data.add(new Tuple3<>(1,2,9));
         data.add(new Tuple3<>(1,2,11));
         data.add(new Tuple3<>(1,2,13));
         DataStreamSource ds = env.fromCollection(data);
         SplitStream split = ds.split(new OutputSelector<Tuple3<Integer, Integer, Integer>>() {
             @Override
             public Iterable<String> select(Tuple3<Integer, Integer, Integer> value) {
                 ArrayList<String> selectStrings = new ArrayList<>();
                 if (value.f0 == 0) {
                     selectStrings.add("split0");
                 } else {
                     selectStrings.add("splitnot0");
                 }
                 return selectStrings;
             }
         });
         split.select("split0").print();
         split.select("splitnot0").printToErr();

         //不能二次split 会报以下错误
         //Exception in thread "main" java.lang.IllegalStateException:
         //官方推荐我们用side-outputs
         // Consecutive multiple splits are not supported. Splits are deprecated. Please use side-outputs.
                /* DataStream split0 = split.select("split0");
                 SplitStream split1 = split0.split(new OutputSelector() {
                     @Override
                     public Iterable<String> select(Object value) {
                         return null;
                     }
                 });
                 split1.print();*/
         env.execute("");
     }
 }
