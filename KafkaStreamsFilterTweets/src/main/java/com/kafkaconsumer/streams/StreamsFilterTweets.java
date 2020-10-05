package com.kafkaconsumer.streams;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import com.google.gson.JsonParser;

public class StreamsFilterTweets {
	private static JsonParser jsonParser = new JsonParser();

	public static void main(String args[]) {
		// create properties
		Properties properties = new Properties();
		properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");
		properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
		properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

		// create topology

		StreamsBuilder streamsBuilder = new StreamsBuilder();

		// input topic

		KStream<String, String> inputTopic = streamsBuilder.stream("tweets");
		KStream<String, String> filteredStream = inputTopic.filter(
				// filter for tweets which has a user of over 10000 followers
				(k, jsonTweet) -> extractUserFollowersInTweet(jsonTweet) > 10000);
		// Stream would go to important_tweets topic
		filteredStream.to("important_tweets");

		// build topology
		KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), properties);
		// create streams application
		streams.start();
	}

	private static Integer extractUserFollowersInTweet(String tweetJson) {
		try {
			return jsonParser.parse(tweetJson).getAsJsonObject().get("user").getAsJsonObject().get("followers_count")
					.getAsInt();
		} catch (NullPointerException e) {
			return 0;
		}
	}
}
