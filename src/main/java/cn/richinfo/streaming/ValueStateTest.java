package cn.richinfo.streaming;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.util.Collector;

public class ValueStateTest extends BaseStreaming{
    public static void main(String[] args) throws Exception {
        DataStreamSource<String> ds = env.fromElements("1,12 2,23 3,435 1,657 2,34 3,23 1,1 2,34 3,56 1,7 2,43" +
                " 3,3 1,2 2,354 3,56  1,67 2,34 3,2");
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
        dskeyedstream.flatMap(new RichFlatMapFunction<Tuple2<Integer, Integer>, Object>() {
            private ValueState<Long> valueState;
            @Override
            public void open(Configuration parameters) throws Exception {

            }
            @Override
            public void flatMap(Tuple2<Integer, Integer> value, Collector<Object> out) throws Exception {

            }
        });
        env.execute("");
    }

    @Override
    public  void exe() {
    };
}
