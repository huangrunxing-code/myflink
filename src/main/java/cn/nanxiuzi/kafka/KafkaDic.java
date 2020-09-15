package cn.nanxiuzi.kafka;

import cn.nanxiuzi.annotation.FiledAnnotations;

import java.time.Duration;

public class KafkaDic {
    @FiledAnnotations(describe = "kafka地址")
    public static final String Kafka_ADDRESS_COLLECTION = "rhel075:9092,rhel076:9092,rhel079:9092";
    @FiledAnnotations(describe = "zookeeper列表")
    public static final String Zookeeper_List="rhel072:2181";
    @FiledAnnotations(describe = "消费者连接的topic")
    public static final String CONSUMER_TOPIC = "hrxtopic";
    @FiledAnnotations(describe = "生产者连接的topic")
    public static final String PRODUCER_TOPIC = "hrxtopic";
    @FiledAnnotations(describe = "Flink生产者连接的topic")
    public static final String FLINK_PRODUCER_TOPIC = "flink_hrxtopic";
    @FiledAnnotations(describe = "groupId，可以分开配置")
    public static final String CONSUMER_GROUP_ID = "1";
    @FiledAnnotations(describe = "是否自动提交（消费者）")
    public static final String CONSUMER_ENABLE_AUTO_COMMIT = "true";
    @FiledAnnotations(describe = "消费者自动提交间隔时间")
    public static final String CONSUMER_AUTO_COMMIT_INTERVAL_MS = "1000";
    @FiledAnnotations(describe = "连接超时时间")
    public static final String CONSUMER_SESSION_TIMEOUT_MS = "30000";
    @FiledAnnotations(describe = "每次拉取数")
    public static final int CONSUMER_MAX_POLL_RECORDS = 10;
    @FiledAnnotations(describe = "拉去数据超时时间")
    public static final Duration CONSUMER_POLL_TIME_OUT = Duration.ofMillis(3000);
}
