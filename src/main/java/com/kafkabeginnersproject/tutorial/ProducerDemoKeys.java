package com.kafkabeginnersproject.tutorial;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerDemoKeys {

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		// create producer properties
		Properties properties=new Properties();
		String bootStrapServers="127.0.0.1:9092";
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
		properties.setProperty("key.serializer", StringSerializer.class.getName());
		properties.setProperty("value.serializer", StringSerializer.class.getName());
		
		// create producer
		KafkaProducer<String, String> producer=new KafkaProducer<String, String>(properties);
		for(int i=0;i<10;i++) {
		String key="id_"+i;
		//create a producer Record
		ProducerRecord<String, String> record =new ProducerRecord<String, String>("first_topic",key, "Helloooo!");
		
		
		// send data 
		producer.send(record).get();//block .send() to make it asynchronous- don't do this in Production
		}
		//flush and close
		producer.close(); 
	}

}
