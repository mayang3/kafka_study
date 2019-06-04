package consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

@SuppressWarnings("Duplicates")
public class KafkaConsumerManual {
	public static void main(String[] args) {
		Properties props = new Properties();

		props.put("bootstrap.servers", "dev-neo-kk01-ncl:9092,dev-neo-kk02-ncl:9092,dev-neo-kk03-ncl:9092");
		props.put("group.id", "jb-manual");
		props.put("enable.auto.commit", "false"); // 최초 자동 커밋을 끄게끔 설정해두고
		props.put("auto.offset.reset", "latest");
		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", StringDeserializer.class.getName());

		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

		consumer.subscribe(Arrays.asList("neo-topic"));

		try {

			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

				for (ConsumerRecord<String, String> record: records) {
					System.out.printf("Topic:%s, Partition:%s, Offset:%s, Key:%s, Value:%s\n", record.topic(), record.partition(), record.offset(), record.key(), record.value());
				}

				// 메세지를 모두 가져온 후, 자동으로 커밋한다.
				consumer.commitSync();
			}

		} finally {
			consumer.close();
		}
	}
}
