package com.taha.kafka;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * Created by NAQVI on 11/23/2019.
 */

// RUN THIS MULTIPLE TIME TO SEE HOW PARTITIONS OF THE TOPIC GET RE-BALANCED TO DIFFERENCT CONSUMERS IN SAME CONSUMER GROUP.
public class ConsumerTestGroups {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerTestGroups.class);

        //create  consumer configs
        String boostrapServers="127.0.0.1:9092";
        String groupId="My App from IJ 2";
        Properties properties =new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,boostrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");


        //create consumer

        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        //subscribe  consumer to our topics(s)
        //for single or one topic
       // kafkaConsumer.subscribe(Collections.singleton("TahaTopic"));
        kafkaConsumer.subscribe(Arrays.asList("TahaTopic"));

        //poll for new data
        while (true){
           // kafkaConsumer.poll(100); deprecated
            ConsumerRecords<String,String> consumerRecord = kafkaConsumer.poll(Duration.ofMillis(100)); //new in Kafka 2.2.0


            for(ConsumerRecord<String,String> records : consumerRecord){
                logger.info("key :"+ records.key() + "value : "+records.value());
                logger.info("offset :"+ records.offset() + "partition: "+records.partition());

            }
                
        }
    }
}
