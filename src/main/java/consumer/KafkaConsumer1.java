package consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * @author neo82
 */
public class KafkaConsumer1 {
	public static void main(String[] args) {
		Properties props = new Properties();

		props.put("bootstrap.servers", "dev-jb-kk001-ncl:9092,dev-jb-kk002-ncl:9092,dev-jb-kk003-ncl:9092");
		props.put("group.id", "jb-consumer");
		props.put("enable.auto.commit", true); // background 에서 주기적으로 offset 을 commit 한다.
		props.put("auto.offset.reset", "latest"); // offset 이 존재하지 않을 경우 어디서 가져올 것인가
		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", StringDeserializer.class.getName());

		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

		consumer.subscribe(Arrays.asList("jb-pub-topic"));

		try {
			while (true) {
				// consumer 가 kafka 에 polling 할 blocking time
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

				// 한번에 여러개의 records 를 가져올 수 있다. -> max.poll.records 옵션을 통해 제어가 가능하다.
				for (ConsumerRecord<String, String> record : records) {
					System.out.printf("Topic:%s, Partition:%s, Offset:%s, Key:%s, Value:%s\n", record.topic(), record.partition(), record.offset(), record.key(), record.value());
				}

			}
		} finally {
			// 호출 즉시 리밸런스가 일어난다.
			consumer.close();
		}

	}
}
