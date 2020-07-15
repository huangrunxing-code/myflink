package cn.richinfo.streaming;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public abstract class BaseStreaming {
    protected static   StreamExecutionEnvironment env;
    static{
        env = StreamExecutionEnvironment.getExecutionEnvironment();
    }
    public  abstract   void exe();
}
