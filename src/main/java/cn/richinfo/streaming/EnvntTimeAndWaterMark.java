package cn.richinfo.streaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;

public class EnvntTimeAndWaterMark {
    public static void main(String[] args) throws Exception {
      
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置水印生成时间间隔100ms
        env.getConfig().setAutoWatermarkInterval(100);
        DataStreamSource<String> socds = env.socketTextStream("192.168.42.71", 9009);
        //分配时间戳
        DataStream<String> ds = socds.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<String>() {
            @Nullable
            //当前水印时间戳
            private Long currentTimeStamp = 0L;
            //最大的允许乱序时间  5s
            private Long maxOutofOrderness = 2000L;

            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(currentTimeStamp - maxOutofOrderness);
            }

            @Override
            //从数据里面提取时间戳
            public long extractTimestamp(String element, long previousElementTimestamp) {
                Long mytime=Long.parseLong(element.split(",")[1]);
                currentTimeStamp=Math.max(mytime, currentTimeStamp);
                //数据样例   name|timestamp
                /*  flink,1588659181000
                    flink,1588659182000
                    flink,1588659183000
                    flink,1588659184000
                    flink,1588659185000
                    flink,1588659186000

                    flink,1588659181000
                    flink,1588659182000
                    flink,1588659183000
                    flink,1588659184000
                    flink,1588659185000
                    flink,1588659180000
                    flink,1588659186000
                    flink,1588659187000
                    flink,1588659188000
                    flink,1588659189000
                    flink,1588659190000
                    */
                System.err.println("===============mytime"+mytime+"===========watermark:"+(currentTimeStamp - maxOutofOrderness));

                return mytime;
            }
        });
        ds.map(new MapFunction<String, Tuple2<String,Long>>() {
            @Override
            public Tuple2<String,Long> map(String s) throws Exception {
                String[] split = s.split(",");
                return new Tuple2<String,Long>(split[0],Long.parseLong(split[1])) ;
            }
        }).keyBy(0)
          .window(TumblingEventTimeWindows.of(Time.seconds(3)))
           .minBy(1)
           .printToErr();
        env.execute("watermark");
    }
}
