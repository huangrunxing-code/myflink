package cn.richinfo;

import akka.stream.impl.fusing.Collect;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class operate {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds = env.fromElements("年后", "asdsad", "as阿萨德dsad", "asds 二ad", "as撒旦 法dsad", "asd让 他sad", "as受到dsad");
        DataStreamSource<String> ds2 = env.fromElements(
                "To be, or not to be,--that is the question:--",
                "Whether 'tis nobler in the mind to suffer",
                "The slings and arrows of outrageous fortune",
                "Or to take arms against a sea of troubles,");
        // ds.map((e)->e+"300").print();
   /*     ds.map((String e)->{
           if(e.equals("年后")){
               return e+"=====>年后";
           } else{
               return e+"==========>不是年后0";
           }
        }).print();*/
        ds.flatMap((String e, Collector<String> out) -> {
            String[] split = e.split("\\s+");
            for (String s: split) {
                out.collect(s);
            }
        }).returns(new TypeHint<String>() {
        }).print();
        env.execute("test");
    }
}
