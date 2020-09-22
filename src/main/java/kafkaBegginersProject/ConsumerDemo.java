package kafkaBegginersProject;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemo {

	public static void main(String[] args) {
		
    Logger logger=LoggerFactory.getLogger(ConsumerDemo.class.getName());
    String bootstrapServers="127.0.0.1:9092";
    String groupId="my-application";
    //create consumer configuration
    Properties properties=new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    //earliest/latest/none
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
     
    //create consumer
    KafkaConsumer<String, String> consumer=new KafkaConsumer<String, String>(properties);
    
    //subscribe consumer to our topic(s)
    consumer.subscribe(Collections.singleton("first_topic"));
    
    //poll for new data
    while(true) {
    	ConsumerRecords<String, String> records=consumer.poll(Duration.ofMillis(100));
    	
    	for(ConsumerRecord<String,String> record:records) {
    		logger.info("key:"+record.key()+" value:"+record.value());
    		logger.info("Partition:"+record.partition()+" Offset"+record.offset());
    	}
    }
	}

}
