package cn.nanxiuzi.streaming;

import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;

public class AsyncDBFunction implements AsyncFunction {
    @Override
    public void asyncInvoke(Object input, ResultFuture resultFuture) throws Exception {

    }
}
