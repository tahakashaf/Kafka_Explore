package com.taha.kafka;

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

/**
 * Created by NAQVI on 11/24/2019.
 */
class ConsumerThread implements Runnable{

    private CountDownLatch latch;
    private KafkaConsumer<String,String> kafkaConsumer ;
    private Logger logger = LoggerFactory.getLogger(ConsumerThread.class.getName());


    public ConsumerThread(String bootstrapServers,
                          String groupId,
                          String topic,
                          CountDownLatch latch) {
        this.latch = latch;
        //create  consumer configs

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create consumer
        kafkaConsumer = new KafkaConsumer<String, String>(properties);
        // subscribe consumer to our topic(s)
        kafkaConsumer.subscribe(Arrays.asList(topic));


    }

    @Override
    public  void run(){
        //poll for new data
        try {
            while (true) {
                // kafkaConsumer.poll(100); deprecated
                ConsumerRecords<String, String> consumerRecord = kafkaConsumer.poll(Duration.ofMillis(100)); //new in Kafka 2.2.0


                for (ConsumerRecord<String, String> records : consumerRecord) {
                    logger.info("key :" + records.key() + "value : " + records.value());
                    logger.info("offset :" + records.offset() + "partition: " + records.partition());

                }

            }
        }catch (WakeupException e){
            logger.info("Received Shutdown Signal");


        }finally {
            kafkaConsumer.close();
            //tell our main code that we are done with our consumer
            latch.countDown();
        }
    }

    public void shutDownThread(){
        //the wakeup() method is a special method to interrupt consumer.poll()
        // it will throw the exception WakeUpException
        kafkaConsumer.wakeup();

    }
}


