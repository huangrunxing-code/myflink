package cn.richinfo.streaming;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 功能描述:
 *
 * @ClassName: TestAggreageFunction
 * @Author: huangrx丶
 * @Date: 2020/7/15 20:44
 * @Version: V1.0
 */
public class TestAggreageFunction {

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
        DataStreamSource<Tuple3<String, String, Long>> ds = env.fromElements(CLASS_SCORE);
        ds.keyBy(0)
          .countWindow(2)
          .aggregate(new MyAggreageFunction())
          .print();
        env.execute("开始执行");
    }

    //sum/count
    private static class MyAggreageFunction implements AggregateFunction<Tuple3<String, String, Long>, Tuple2<Long, Long>, Double> {

        //创建累加器  就是父类的那个第二个参数  直接新建就可以了
        @Override
        public Tuple2<Long, Long> createAccumulator() {
            //这里要赋初始值，要不然后面add会报Caused by: java.lang.NullPointerException
            return new Tuple2<Long, Long>(0l,0l);
        }

        //累加器的逻辑  每一次触发window的时候用的
        @Override
        public Tuple2<Long, Long> add(Tuple3<String, String, Long> value, Tuple2<Long, Long> accumulator) {
            return new Tuple2<Long, Long>(accumulator.f0 + value.f2, accumulator.f1 + 1L);
        }

        /***
         * @Author huangrunxing
         * @Description //TODO 计算返回的那个结果  每一次触发window计算结束的时候返回的那个
         * @Date 2020/7/15 20:54
         * @Param [accumulator]
         * @return java.lang.Double
         **/
        @Override
        public Double getResult(Tuple2<Long, Long> accumulator) {
            return accumulator.f0.doubleValue() / accumulator.f1;
        }

        /**
         * @return org.apache.flink.api.java.tuple.Tuple2<java.lang.Long       ,       java.lang.Long>
         * @Author huangrunxing
         * @Description //TODO 两个累加器相加的逻辑
         * @Date 2020/7/15 20:53
         * @Param [a, b]
         **/
        @Override
        public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
            return new Tuple2<Long, Long>(a.f0 + b.f0, a.f1 + b.f1);
        }
    }


}
