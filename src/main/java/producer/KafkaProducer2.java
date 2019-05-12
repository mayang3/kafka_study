package producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * @author neo82
 */
public class KafkaProducer2 {
	public static void main(String[] args) {
		Properties props = new Properties();

		props.put("bootstrap.servers", "dev-jb-kk001-ncl:9092,dev-jb-kk002-ncl:9092,dev-jb-kk003-ncl:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<String, String>(props);

		try {
			RecordMetadata recordMetadata = producer.send(new ProducerRecord<String, String>("jb-pub-topic", "Kafka Client Sync Send")).get();

			System.out.println(recordMetadata.partition());
			System.out.println(recordMetadata.offset());

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			producer.close();
		}

	}
}
