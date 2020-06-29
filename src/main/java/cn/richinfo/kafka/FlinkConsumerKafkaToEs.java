package cn.richinfo.kafka;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;

import java.util.Properties;

public class FlinkConsumerKafkaToEs {
    private static String HDFS_PATH="hdfs://nameservice1/tmp";
    private static String SOURCE_SPLIT_STR=",";
    private static String TARGIT_SPLIT_STR="|";
    public static void main(String[] args) {
        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
      //  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //实例source object
        Properties props = new Properties();
        props.setProperty("bootstrap.servers",KafkaDic.Kafka_ADDRESS_COLLECTION);
        props.setProperty("zookeeper.connect",KafkaDic.Zookeeper_List);
        props.setProperty("group.id",KafkaDic.CONSUMER_GROUP_ID);
        FlinkKafkaConsumer010<String> ksource = new FlinkKafkaConsumer010<>(KafkaDic.CONSUMER_TOPIC, new SimpleStringSchema(), props);
        DataStreamSource<String> sdatastream = env.addSource(ksource);
        //transform
        SingleOutputStreamOperator<String> map = sdatastream.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                System.out.println(s);
                String[] split = s.split(SOURCE_SPLIT_STR);
                StringBuilder tmpstr=new StringBuilder();
                for(String str:split){
                    if(tmpstr.length()>0){
                        tmpstr.append(TARGIT_SPLIT_STR).append(str);
                    }else{
                        tmpstr.append(str);
                    }
                }
                return tmpstr.toString();
            }
        });
        //sink to other
        FlinkKafkaProducer010<String> ksink = new FlinkKafkaProducer010<String>(
                KafkaDic.Kafka_ADDRESS_COLLECTION,
                KafkaDic.FLINK_PRODUCER_TOPIC,
                new SimpleStringSchema()
        );
        map.addSink(ksink);

        //执行
        try {
            env.execute("FlinkConsumerKafkaToKafka");
        } catch (Exception e) {
            //ignore
        }
    }
}
