package com.seth.java.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerWithThreadDemo {

    public static void main(String[] args) {
        new ConsumerWithThreadDemo().run();
    }

    private ConsumerWithThreadDemo(){}

    private void run (){
        Logger logger = LoggerFactory.getLogger(ConsumerWithThreadDemo.class);
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-seventh-application";
        String topic = "first_topic";

        CountDownLatch latch = new CountDownLatch(1);
        logger.info("Creating the Consumer Thread");
        Runnable myConsumerRunnable = new ConsumerRunnable(bootstrapServers, groupId, topic, latch);
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught Shutdown hook");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        } finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable {
        Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);
        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;

        public ConsumerRunnable(String bootstrapServers, String groupId, String topic, CountDownLatch latch){
            this.latch = latch;
            // Create consumer config
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            // Create consumer
            this.consumer = new KafkaConsumer<String, String>(properties);

            // subscribe consumer to our topics
            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {

            try{
                //poll for new data
                while(true){
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for(ConsumerRecord<String, String> record : records){
                        logger.info("Key: " + record.key() + "\n" +
                                "Value: " + record.value() + "\n" +
                                "Partition: " + record.partition() + "\n" +
                                "Offset: " + record.offset());
                    }
                }
            } catch (WakeupException e){
                logger.info("Received shutdown signal");
            } finally {
                consumer.close();
                // tell our main code we're done with the consumer
                latch.countDown();
            }

        }

        public void shutdown(){
            // the wakeup() method is a special method to interrupt consumer.poll()
            // it will throw the exception WakeUpException
            consumer.wakeup();
        }
    }
}
