package com.kafkabeginnersproject.tutorial;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerDemo {

	public static void main(String[] args) {
		// create producer properties
		Properties properties=new Properties();
		String bootStrapServers="127.0.0.1:9092";
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
		properties.setProperty("key.serializer", StringSerializer.class.getName());
		properties.setProperty("value.serializer", StringSerializer.class.getName());
		
		// create producer
		KafkaProducer<String, String> producer=new KafkaProducer<String, String>(properties);
		
		//create a producer Record
		ProducerRecord<String, String> record =new ProducerRecord<String, String>("first_topic", "hello world!");
		
		
		// send data -asynchronous
		producer.send(record);
		//flush and close
		producer.close(); 
	}

}
