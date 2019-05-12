package producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author neo82
 */
public class KafkaProducerKey {
	public static void main(String[] args) throws ExecutionException, InterruptedException {
		Properties props = new Properties();

		props.put("bootstrap.servers", "dev-jb-kk001-ncl:9092,dev-jb-kk002-ncl:9092,dev-jb-kk003-ncl:9092");
		props.put("acks", "1");
		props.put("compression.type", "gzip");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<String, String>(props);

		String testTopic = "jb-keys-topic";
		String oddKey = "1";
		String evenKey = "2";

		for (int i = 1; i < 11; i++) {
			if (i % 2 == 1) {
				RecordMetadata recordMetadata = producer.send(new ProducerRecord<String, String>(testTopic, oddKey, "odd Key:" + i)).get();

				System.out.println(recordMetadata.topic());
				System.out.println(recordMetadata.partition());
			} else {
				RecordMetadata recordMetadata = producer.send(new ProducerRecord<String, String>(testTopic, evenKey, "even Key:" + i)).get();

				System.out.println(recordMetadata.topic());
				System.out.println(recordMetadata.partition());

				// 여기서 future get 을 안해주면 메인스레드가 종료되어 실제 topic 에 메세지가 전송되지 않을 수 있다.
			}
		}

	}
}
