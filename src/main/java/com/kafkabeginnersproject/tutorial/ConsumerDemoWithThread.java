package com.kafkabeginnersproject.tutorial;
 
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
 
public class ConsumerDemoWithThread {
    public static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    public static final String GROUP_ID = "my-sixth-application";
    public static final String AUTO_OFFSET_RESET = "earliest";
    public static final String TOPIC = "first_topic";
 
    private ConsumerDemoWithThread() {
    }
 
    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }
 
    private void run() {
        final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);
        final CountDownLatch latch = new CountDownLatch(1);
        logger.info("creating the consumer runnable");
        final Runnable consumerRunnable = new ConsumerRunnable(
                BOOTSTRAP_SERVERS,
                GROUP_ID,
                AUTO_OFFSET_RESET,
                TOPIC,
                latch);
 
        // start the thread with consumer runnable
        Thread consumerThread = new Thread(consumerRunnable);
        consumerThread.start();
 
        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("caught shutdown hook");
            ((ConsumerRunnable) consumerRunnable).shutdown();
 
            //wait for consumer runnable thread
            try {
                latch.await();
            } catch (InterruptedException e) {
                logger.error("Ups...", e);
            }
 
            logger.info("application has exited");
        }));
    }
 
 
    public class ConsumerRunnable implements Runnable {
        final private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);
        final private AtomicBoolean hasToShutdown = new AtomicBoolean(false);
        final private KafkaConsumer<String, String> consumer;
        final CountDownLatch latch;
 
        public ConsumerRunnable(final String bootstrapServers,
                                final String groupId,
                                final String autoOffsetReset,
                                final String topic,
                                final CountDownLatch latch) {
            this.latch = latch;
            // create kafka consumer
            consumer = new KafkaConsumer<>(createConfigs(bootstrapServers, groupId, autoOffsetReset));
            // subscribe to our topic(s)
            consumer.subscribe(Collections.singletonList(topic));
        }
 
        // create kafka consumer configs
        private Properties createConfigs(final String bootstrapServers,
                                         final String groupId,
                                         final String autoOffsetReset) {
 
            final Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
 
            return properties;
        }
 
        @Override
        public void run() {
            try {
                // poll for new data
                while (!hasToShutdown.get()) {
                    ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
                    consumerRecords.forEach(consumerRecord -> logger.info(String.valueOf(consumerRecord)));
                }
            } catch (WakeupException e) {
                if(hasToShutdown.get()) {
                    logger.info("received shutdown signal");
                } else {
                    logger.error("Hmm, ...", e);
                }
            } finally {
                logger.info("closing kafka consumer...");
                consumer.close();
                logger.info("kafka consumer closed");
                latch.countDown();
            }
        }
 
        public void shutdown() {
            logger.info("shutdown requested");
            // the wakeup() method is a special method to interrupt consumer.poll()
            // it will throw the WakeUpException
             hasToShutdown.set(true);
            consumer.wakeup();
        }
    }
}
 
