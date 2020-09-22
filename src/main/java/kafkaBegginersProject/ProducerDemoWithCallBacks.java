package kafkaBegginersProject;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoWithCallBacks {

	public static void main(String[] args) {

		final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallBacks.class);

		// create producer properties
		Properties properties = new Properties();
		String bootStrapServers = "127.0.0.1:9092";
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
		properties.setProperty("key.serializer", StringSerializer.class.getName());
		properties.setProperty("value.serializer", StringSerializer.class.getName());

		// create producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
 
		for(int i=0;i<10;i++) {
		
		// create a producer Record
		ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello world"+i);

		// send data -asynchronous
		producer.send(record, new Callback() {

			public void onCompletion(RecordMetadata metadata, Exception exception) {
				// Executes every time a record is sent or an Exception is thrown
				if (null == exception) {
					logger.info("Recieved new metadata" + "\n" + "Topic:" + metadata.topic() + "\n" + "Partition:"
							+ metadata.partition() + "\n" + "Offset:" + metadata.offset() + "\n" + "Timestamp:"
							+ metadata.timestamp() + "\n");
				} else {
					logger.error("Recieved Error while producing", exception);
				}
			}
		});
		
		}	
		// flush and close
		producer.close();
	}

}
