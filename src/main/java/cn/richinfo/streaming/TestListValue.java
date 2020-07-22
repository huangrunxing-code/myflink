package cn.richinfo.streaming;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 功能描述:
 *
 * @ClassName: TestListValue
 * @Author: huangrx丶
 * @Date: 2020/7/21 22:45
 * @Version: V1.0
 */
public class TestListValue {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<Long> inputStream = env.fromElements(1l, 2l, 3l, 4l, 1l, 2l, 4l, 5l, 7l, 1l, 2l, 1l, 2l, 3l, 5l, 1l);
    }
}
