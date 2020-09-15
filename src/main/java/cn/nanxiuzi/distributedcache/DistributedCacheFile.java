package cn.nanxiuzi.distributedcache;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.util.ArrayList;
import java.util.List;


/**
 * 练习分布式缓存文件
 */
public class DistributedCacheFile {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //注册分布式文件，可以是hdfs或者是本地的
        env.registerCachedFile("F:\\distributedfile.txt","mydistributedfile");
        DataSource<String> dt = env.fromElements("hrx", "heqin", "hyx", "hmx");
        MapOperator<String, Object> mydistributedfile = dt.map(new RichMapFunction<String, Object>() {
            private ArrayList<String> filelist = new ArrayList<String>();

            @Override
            public void open(Configuration parameters) throws Exception {
                //使用缓存文件
                super.open(parameters);
                File mydistributedfile = getRuntimeContext().getDistributedCache().getFile("mydistributedfile");
                List<String> strings = FileUtils.readLines(mydistributedfile);
                for (String s : strings) {
                    this.filelist.add(s);
                    System.out.println("当前行分布式缓存文件内容是: " + s);
                }
            }

            @Override
            public Object map(String value) throws Exception {
                //在这里就可以使用dataList
                System.err.println("使用datalist：" + filelist + "-------" + value);
                //业务逻辑
                return filelist + "：" + value;
            }
        });
        mydistributedfile.printToErr();

    }
}
