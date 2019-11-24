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
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

/**
 * Created by NAQVI on 11/23/2019.
 */

// Lear about this more
public class ConsumerTestGroupsThreads {
    public static void main(String[] args) {

         new ConsumerTestGroupsThreads().run();

    }
    private  ConsumerTestGroupsThreads(){

    }
    private  void run() {
        Logger logger = LoggerFactory.getLogger(ConsumerTestGroupsThreads.class);
        String boostrapServers = "127.0.0.1:9092";
        String groupId = "My App threaded";
        String topic = "TahaTopic";
        CountDownLatch countDownLatch = new CountDownLatch(1);
        ConsumerThread consumerThread = new ConsumerThread(boostrapServers, groupId, topic, countDownLatch);
        // start the thread
        Thread myThread = new Thread(consumerThread);
        myThread.start();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            ((ConsumerThread) consumerThread).shutDownThread();
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }

        ));

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        } finally {
            logger.info("Application is closing");
        }
    }

    }

