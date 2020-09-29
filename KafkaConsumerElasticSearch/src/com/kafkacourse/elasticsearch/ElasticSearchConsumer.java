package com.kafkacourse.elasticsearch;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticSearchConsumer {

	public static RestHighLevelClient createClient() {

		String hostName = "kafkapoc-932356640.ap-southeast-2.bonsaisearch.net";
		String userName = "9y49pea4n7";
		String password = "3bcwej3w9b";

		// Do not do, if you are running a local Elastic search instance
		final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName, password));
		RestClientBuilder builder = RestClient.builder(new HttpHost(hostName, 443, "https"))
				.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {

					public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
						return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
					}
				});
		RestHighLevelClient client = new RestHighLevelClient(builder);
		return client;
	}

	public static void main(String[] args) throws IOException {
         
		Logger logger=LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
		
		RestHighLevelClient client =createClient();
		String jsonString= "{\"foo\":\"bar\"}";
		
		IndexRequest indexRequest=new IndexRequest("twitter","tweets").source(jsonString,XContentType.JSON);
		IndexResponse indexResponse=client.index(indexRequest, RequestOptions.DEFAULT);
		
		String id=indexResponse.getId();
		logger.info(id);

		KafkaConsumer<String, String> consumer=createConsumer("tweets");
		
		 //poll for new data
	    while(true) {
	    	ConsumerRecords<String, String> records=consumer.poll(Duration.ofMillis(100));
	    	
	    	for(ConsumerRecord<String,String> record:records) {
	    	}
	    }
		
		//client.close();
	}
	public static KafkaConsumer<String, String> createConsumer(String topic){
		
		String bootstrapServers="127.0.0.1:9092";
	    String groupId="kafka-demo-elasticsearch";
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
	    consumer.subscribe(Collections.singleton(topic));
	    return consumer;
	}

}