package consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * @author neo82
 */
@SuppressWarnings("Duplicates")
public class KafkaConsumerPartition {
	public static void main(String[] args) {

		Properties props = new Properties();

		props.put("bootstrap.servers", "dev-jb-kk001-ncl:9092,dev-jb-kk002-ncl:9092,dev-jb-kk003-ncl:9092");
		// partition 을 지정하는데, group id 를 같게 한다면..
		// 컨슈머 마다 할당된 파티션에 대한 오프셋 정보를 서로 공유하기 때문에, 원치 않는 형태로 동작할 수 있다.
		props.put("group.id", "jb-partition");
		props.put("enable.auto.commit", "false");
		props.put("auto.offset.reset", "latest");
		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer",StringDeserializer.class.getName());

		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

		String topic = "jb-pub-topic";

		TopicPartition partition0 = new TopicPartition(topic, 0);
		TopicPartition partition1 = new TopicPartition(topic, 1);

		// consumer.subscribe 대신에 partition 을 직접 assign 해준다.
		consumer.assign(Arrays.asList(partition0, partition1));

		// 특정 offset 부터 메세지 가져오고자 하는 경우
//		consumer.seek(partition0, 2);
//		consumer.seek(partition1, 2);

		try {
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

				for (ConsumerRecord<String, String> record : records) {
					System.out.printf("Topic:%s, Partition:%s, Offset:%s, Key:%s, Value:%s\n", record.topic(), record.partition(), record.offset(), record.key(), record.value());
				}

				consumer.commitSync();
			}
		} finally {
			consumer.close();
		}
	}
}
