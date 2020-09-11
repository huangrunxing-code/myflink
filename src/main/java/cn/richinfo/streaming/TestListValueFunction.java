package cn.richinfo.streaming;

import org.apache.flink.api.common.functions.RichFlatMapFunction; 
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.types.ListValue;
import org.apache.flink.util.Collector;

import java.util.List;

/***
 * @Author huangrunxing
 * @Description //TODO
 * @Date  2020/7/21 22:57
 * @Param
 * @return
 * 获取两次事件1之间 有多少次事件 分别是什么事件
 * 输入：
 * 1 2 3 4 1 3 4 1 2 3 2 3 4 5 1
 * 输出：
 * 3，2 3 4
 * 2，3 4
 * 6,2 3 2 3 4 5
 **/
public class TestListValueFunction extends RichFlatMapFunction<Long, Tuple2<Integer,String>> implements CheckpointedFunction {
    //定义一个托管状态 家伙是哪个teansient 不要让他序列化到磁盘中
    private transient ListValue<Long> checkpointList;

    //定义一个原始状态
    private List<Long> originList;

    @Override
    public void flatMap(Long value, Collector<Tuple2<Integer, String>> out) throws Exception {
        if(value==1l){
            //除了第一次之外都要开始做输出了
            if(originList.size()>0){
                for(Long l : originList){

                }
            }
        }else{
            originList.add(value);
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {

    }
}
