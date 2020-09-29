package com.kafkacourse.twitterproducer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterProducer {
	org.slf4j.Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
	String consumerKey = "knAzlXs8W5X1VjeWAPsbRcZdI";
	String consumerSecret = "YJA1sjEjp7nu2uajwSSgrqgrfhFufqQ0YsE4U79O4dmkUmoZSd";
	String token = "1309339088076627968-ByXtB2Tw2Cm0OwqNrCFr0zMQA9HXYZ";
	String secret = "dqxcuOz5QOGT3HUMiixqu3S7n2y7f2LSrhZadDeJfKAvk";

	public TwitterProducer() {

	}

	public static void main(String args[]) {
		new TwitterProducer().run();
	}

	public void run() {
		/**
		 * Set up your blocking queues: Be sure to size these properly based on expected
		 * TPS of your stream
		 */
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

		// create a twitter client
		// Attempts to establish a connection.
		Client client = createTwitterClient(msgQueue);
		client.connect();

		// create a kafka Producer
		KafkaProducer<String, String> producer = createKafkaProducer();

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			logger.info("Adding shutdown hook");
			logger.info("stopping hosebird client");
			client.stop();
			logger.info("closing producer");
			producer.close();
			logger.info("done");
		}) 

		);

		// loop to send tweets to kafka
		while (!client.isDone()) {
			String msg = null;
			try {
				msg = msgQueue.poll(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				e.printStackTrace();
				client.stop();
			}
			if (null != msg) {
				logger.info(msg);
				ProducerRecord<String, String> record = new ProducerRecord<String, String>("Tweets", null, msg);

				// send data -asynchronous
				producer.send(record, new Callback() {

					public void onCompletion(RecordMetadata metadata, Exception exception) {
						if (null != exception) {
							logger.info("Exception occured" +exception);
						}
					}
				});
			}
		}
		logger.info("End of app");
	}

	private KafkaProducer<String, String> createKafkaProducer() {

		// create producer properties
		Properties properties = new Properties();
		String bootStrapServers = "127.0.0.1:9092";
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
		properties.setProperty("key.serializer", StringSerializer.class.getName());
		properties.setProperty("value.serializer", StringSerializer.class.getName());

		//set properties for safe producer
		properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
		properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
		//kafka 2.0 >1, hence can use 5, use 1 otherwise
		properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
		
		//high throughput producer(at the expense of a bit of latency and CPU usage)
		properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
		properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32*1024));
		
		
		// create producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

		return producer;
	}

	public Client createTwitterClient(BlockingQueue<String> msgQueue) {

		/**
		 * Declare the host you want to connect to, the endpoint, and authentication
		 * (basic auth or oauth)
		 */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		// Optional: set up some followings and track terms
		// List<Long> followings = Lists.newArrayList(1234L, 566788L);
		List<String> terms = Lists.newArrayList("java","covid","vaccine","bjp");
		// hosebirdEndpoint.followings(followings);
		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

		ClientBuilder builder = new ClientBuilder().name("Hosebird-Client-01") // optional: mainly for the logs
				.hosts(hosebirdHosts).authentication(hosebirdAuth).endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue));

		Client hosebirdClient = builder.build();
		return hosebirdClient;
	}
}
