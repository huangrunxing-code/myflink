package cn.nanxiuzi.streaming;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * keyedstate练习计算key相同的连续三个的平均值
 */
public class ValueStateTest   {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //初始化数据源
        DataStreamSource<String> ds = env.fromElements("1,12 2,23 3,435 1,657 2,34 3,23 1,1 2,34 3,56 1,7 2,43" +
                " 3,3 1,2 2,354 3,56  1,67 2,34 3,2");
        //拆分之后生成Tuple2
        KeyedStream<Tuple2<Integer, Integer>, Tuple> dskeyedstream = ds.flatMap((String s, Collector<Tuple2<Integer, Integer>> out) -> {
            String[] split = s.split("\\s+");
            for (String t : split) {
                String[] split1 = t.split(",");
                out.collect(new Tuple2<Integer, Integer>(Integer.parseInt(split1[0]), Integer.parseInt(split1[1])));
            }
        }).returns(new TypeHint<Tuple2<Integer, Integer>>() {
            @Override
            public TypeInformation<Tuple2<Integer, Integer>> getTypeInfo() {
                return super.getTypeInfo();
            }
        }).keyBy(0);

        //要是用实现了Rich接口的算子  才有open方法
        dskeyedstream.flatMap(new RichFlatMapFunction<Tuple2<Integer, Integer>, Object>() {
            private transient ValueState<Tuple2<Long, Long>> sumState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<Tuple2<Long, Long>> sumStateDescripetor =
                        new ValueStateDescriptor<>("sumState", TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
                }));
                sumState = getRuntimeContext().getState(sumStateDescripetor);
            }

            @Override
            public void flatMap(Tuple2<Integer, Integer> value, Collector<Object> out) throws Exception {
                //取值
                Tuple2<Long, Long> sumValue = sumState.value();
                //值修改
                if (sumValue == null) {
                    sumValue = Tuple2.of(0L, 0L);
                }
                sumValue.f0+=1;
                sumValue.f1+=value.f1.longValue();
                //值回写
                sumState.update(sumValue);

                if(sumValue.f0>=3){
                    out.collect(Tuple2.of(value.f0,sumValue.f1/sumValue.f0));
                    //Removes the value mapped under the current key
                    sumState.clear();
                }

            }
        }).print();
        env.execute("");
    } 
}
