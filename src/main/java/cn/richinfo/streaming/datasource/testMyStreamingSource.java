package cn.richinfo.streaming.datasource;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 功能描述:
 *
 * @ClassName: testMyStreamingSource
 * @Author: huangrx丶
 * @Date: 2020/9/11 23:08
 * @Version: V1.0
 */
public class testMyStreamingSource
{
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.addSource(new MyStreamingSource()).setParallelism(1);
        source.print();
        env.execute("test my streaming source");
    }
}
