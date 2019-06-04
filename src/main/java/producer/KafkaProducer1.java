package producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * 메세지를 보내고 확인하지 않기
 *
 * @author neo82
 */
public class KafkaProducer1 {
	public static void main(String[] args) {
		Properties props = new Properties();

		props.put("bootstrap.servers", "dev-neo-kk01-ncl:9092,dev-neo-kk02-ncl:9092,dev-neo-kk03-ncl:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<String, String>(props);

		producer.send(new ProducerRecord<>("neo-topic", "Apache Kafka is a distributed streaming platform"));
		producer.close();
	}
}
