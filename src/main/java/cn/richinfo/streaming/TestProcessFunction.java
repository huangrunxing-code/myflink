package cn.richinfo.streaming;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

public class TestProcessFunction {
    public static final Tuple3[] CLASS_SCORE = new Tuple3[]{
            Tuple3.of("班级1", "李老二", 90L),
            Tuple3.of("班级1", "张老二", 56L),
            Tuple3.of("班级1", "陈老二", 78L),
            Tuple3.of("班级2", "刘老二", 67L),
            Tuple3.of("班级2", "黄老二", 77L),
            Tuple3.of("班级2", "赵老二", 100L)
    };

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple3<String,String,Long>> ds = env.fromElements(CLASS_SCORE);
        ds.keyBy(0).countWindow(2).process(new MyProcessFunction()).print();
        env.execute("1");


    }

    private static class MyProcessFunction extends
            ProcessWindowFunction<Tuple3<String,String,Long>,Double, Tuple, GlobalWindow> {

        @Override
        public void process(Tuple tuple, Context context, Iterable<Tuple3<String, String, Long>> elements, Collector<Double> out) throws Exception {
            Long counts=0l;
            Long sum=0l;
            for(Tuple3 s:elements){
                counts+=1;
                sum+=(Long) s.f2;
            }
            out.collect( sum.doubleValue()/counts);
        }
    }
}
