package com.taha.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Created by NAQVI on 11/17/2019.
 */
public class ProducerTestWithKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //System.out.print("Hello World");
        final Logger  logger = LoggerFactory.getLogger(ProducerTestWithKeys.class);
        //1.create producer properties
         Properties propeties =new Properties();
         String boostrapServers="127.0.0.1:9092";

         /* OLD way
         propeties.setProperty("bootstrap.servers",boostrapServers);
         propeties.setProperty("key.serializer", StringSerializer.class.getName());
         propeties.setProperty("value.serializer",StringSerializer.class.getName());
        */

        //better way to code
        propeties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,boostrapServers);
        propeties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        propeties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());



        //2.create producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(propeties);

        //create a producer record
        for(int i=0;i<=10;i++) {
            String topic="TahaTopic";
            String value="hi" + Integer.toString(i);
            String key ="Key_" + Integer.toString(i);
            logger.info("Key :"+key);
            final ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, value);

            //send data is asynchronous
            //without callback
            //kafkaProducer.send(record);

            //with callback
            kafkaProducer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes everytime the record is successfully sent or an exception is thrown
                    if (e == null) {
                        logger.info("Received New Metadata" + "\n"
                                + "Topic =" + recordMetadata.topic() + "\n"
                                + "Partition=" + recordMetadata.partition() + "\n"
                                + "Offset= " + recordMetadata.offset() + "\n"
                                + "Timestamp=" + recordMetadata.timestamp() + "\n"
                        );
                    } else {
                        e.printStackTrace();
                    }
                }
            }).get(); //block the .send() to make it synchronous - dont do this in production
        }

            //flush data
            kafkaProducer.flush();

        //close producer
        kafkaProducer.close();
        System.out.print("ad");

        //3.send data
    }
}
