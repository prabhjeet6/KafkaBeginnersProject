package kafkaBegginersProject;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoAssignAndSeek {

	public static void main(String[] args) {

		Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignAndSeek.class.getName());
		String bootstrapServers = "127.0.0.1:9092";

		// create consumer configuration
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		// earliest/latest/none
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		// create consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

		// assign and seek is mostly used to replay data or fetch a specific message
		// assign
		TopicPartition partitionToReadFrom = new TopicPartition("first_topic", 0);
		consumer.assign(Arrays.asList(partitionToReadFrom));

		// seek
		long offSet=10L;
        consumer.seek(partitionToReadFrom, offSet);
		// poll for new data
        int numberOfMessagesToRead=5;
        int numberOfMessagesReadSoFar=0;
        boolean keepOnReading=true;
		while (keepOnReading) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

			for (ConsumerRecord<String, String> record : records) {
				numberOfMessagesReadSoFar++;
				logger.info("key:" + record.key() + " value:" + record.value());
				logger.info("Partition:" + record.partition() + " Offset" + record.offset());
			}
			if(numberOfMessagesReadSoFar>=numberOfMessagesToRead) {
				keepOnReading=false;
				break;
			}
		}
		logger.info("Exiting the application!");
	}

}
