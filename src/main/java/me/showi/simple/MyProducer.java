package me.showi.simple;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyProducer {

	private static Logger logger = LoggerFactory.getLogger(MyProducer.class.getName());

	public static void main(String[] args) {

		Properties props = new Properties();

		// d集群
		// props.put("bootstrap.servers","d0:9092,d1:9092,d2:9092");

		// q集群
		// props.put("bootstrap.servers", "q1:9092,q2:9092,q3:9092");

		// q0自己搭建的集群
		props.put("bootstrap.servers", "q0:9082,q1:9082,q3:9082");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<>(props);
		for (int i = 0; i < 6; i++) {
			producer.send(new ProducerRecord<String, String>("my-topic-2", Integer.toString(i), Integer.toString(i)));
			logger.debug("发送第 {} 条消息,key:{},value:{} ", i + 1, Integer.toString(i), Integer.toString(i));
		}

		producer.close();
	}
}
