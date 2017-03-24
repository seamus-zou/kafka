package me.showi.simple;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyConsumer {

	private static Logger logger = LoggerFactory.getLogger(MyConsumer.class.getName());

	public static void main(String[] args) {

		Properties props = new Properties();
		// d集群
		// props.put("bootstrap.servers", "d0:9092,d1:9092,d2:9092");
		// q集群
		// props.put("bootstrap.servers", "q1:9092,q2:9092,q3:9092");
		// q0自己搭建的集群,端口都改为了9082
		props.put("bootstrap.servers", "q0:9082,q1:9082,q3:9082");
		props.put("group.id", "mytest-2");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("auto.offset.reset", "earliest");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList("my-topic-2"));
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records) {
				logger.debug("offset = {}, key = {}, value = {}", record.offset(), record.key(), record.value());
			}

		}
	}
}
