package cn.nanxiuzi.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class MyKafkaConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
      props.put("bootstrap.servers", KafkaDic.Kafka_ADDRESS_COLLECTION);
      props.put("group.id",KafkaDic.CONSUMER_GROUP_ID);
      props.put("enable.auto.commit", KafkaDic.CONSUMER_ENABLE_AUTO_COMMIT);
      props.put("auto.commit.interval.ms", KafkaDic.CONSUMER_AUTO_COMMIT_INTERVAL_MS);
      props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
      props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
      KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
      consumer.subscribe(Arrays.asList(KafkaDic.FLINK_PRODUCER_TOPIC));

      while (true) {
          ConsumerRecords<String, String> records = consumer.poll(100);
          for (ConsumerRecord<String, String> record : records)
              System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
      }
    }
}
