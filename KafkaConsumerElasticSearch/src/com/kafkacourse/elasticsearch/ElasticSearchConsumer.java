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
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonParser;

public class ElasticSearchConsumer {

	public static RestHighLevelClient createClient() {
		String hostName = "";
		String userName = "";
		String password = "";

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

		Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

		RestHighLevelClient client = createClient();

		KafkaConsumer<String, String> consumer = createConsumer("tweets");

		// poll for new data
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            int recievedCount=records.count();
			logger.info("recieved "+recievedCount +"records");
			
			BulkRequest bulkRequest=new BulkRequest();
			
			for (ConsumerRecord<String, String> record : records) {
				// here we insert data into elastic search
				// id field is to make the Index request Idempotent as the default delivery
				// semantics
				// for a consumer is at least once, and not providing a unique ID to
				// IndexRequest may result in duplicate
				// records consumed
				// There are two ways of generating a unique id

				// 1. kafka Generic Id
				// String id=record.topic()+"_"+record.partition()+"_"+record.offset();

				// 2. Twitter Feed specific Id
				String id = extractIdFromTweet(record.value());

				IndexRequest indexRequest = new IndexRequest("twitter", "tweets", id).source(record.value(),
						XContentType.JSON);
				
				bulkRequest.add(indexRequest);//add indexRequest to BulkRequest object
				
				/*Not needed in case of BulkRequest, which is faster as well
				 * 
				 * IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);

				logger.info(indexResponse.getId());

				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}*/
				
			}
			if(recievedCount>0) {
			BulkResponse bulkResponses=client.bulk(bulkRequest, RequestOptions.DEFAULT);
			logger.info("Committing the offsets");
			consumer.commitSync();
			logger.info(" offsets have been committed");
			}
		}

		// client.close();
	}

	// used from gson Library
	private static JsonParser jsonParser = new JsonParser();

	private static String extractIdFromTweet(String tweetJson) {

		return jsonParser.parse(tweetJson).getAsJsonObject().get("id_str").getAsString();
	}

	public static KafkaConsumer<String, String> createConsumer(String topic) {

		String bootstrapServers = "127.0.0.1:9092";
		String groupId = "kafka-demo-elasticsearch";
		// create consumer configuration
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		// earliest/latest/none
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
		//To disable auto commit of offsets to be able to commit manually; true by default
		properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		
		//To get only 10 records per poll
		properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "50");
		
		// create consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

		// subscribe consumer to our topic(s)
		consumer.subscribe(Collections.singleton(topic));
		return consumer;
	}

}