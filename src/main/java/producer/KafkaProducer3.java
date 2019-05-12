package producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * @author neo82
 */
public class KafkaProducer3 {
	public static void main(String[] args) {
		Properties props = new Properties();

		props.put("bootstrap.servers", "dev-jb-kk001-ncl:9092,dev-jb-kk002-ncl:9092,dev-jb-kk003-ncl:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		Producer<String, String> producer = new KafkaProducer<String, String>(props);

		try {
			producer.send(new ProducerRecord<String, String>("jb-pub-topic", "Async Message Send"), new JbCallback());
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			producer.close();
		}
	}


	static class JbCallback implements Callback {
		public void onCompletion(RecordMetadata recordMetadata, Exception e) {
			if (recordMetadata == null) {
				System.out.println(recordMetadata);
				return;
			}

			System.out.println(recordMetadata.partition());
			System.out.println(recordMetadata.offset());
		}
	}
}
