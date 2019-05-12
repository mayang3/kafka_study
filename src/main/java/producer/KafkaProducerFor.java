package producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author neo82
 */
public class KafkaProducerFor {
	public static void main(String[] args) {
		Properties props = new Properties();

		props.put("bootstrap.servers", "dev-jb-kk001-ncl:9092,dev-jb-kk002-ncl:9092,dev-jb-kk003-ncl:9092");
		props.put("ack", "1");
		props.put("compression.type", "gzip");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<String, String>(props);

		for (int i = 1; i < 11; i++) {
			producer.send(new ProducerRecord<String, String>("jb-pub-topic", "for-loop msg:" + i));
		}

		producer.close();

	}
}
