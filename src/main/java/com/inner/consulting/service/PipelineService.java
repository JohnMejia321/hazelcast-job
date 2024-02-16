package com.inner.consulting.service;


import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Observable;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.inner.consulting.config.HazelcastConfig;
import com.inner.consulting.config.KafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;


public class PipelineService {


    HazelcastConfig hazelcastConfig= new HazelcastConfig();

    KafkaConfig kafkaConfig= new KafkaConfig();

    public void ejecutarPipelineDesdeKafka() {
        try {
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaConfig.consumerProperties());
            consumer.subscribe(Collections.singletonList("My_topic"));
            consumer.subscribe(Collections.singletonList("topic-job"));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                    // Aquí puedes agregar la lógica para procesar el registro, por ejemplo, enviarlo a Hazelcast
                    // hazelcastInstance.getMap("myMap").put(record.key(), record.value());
                }
            }
        } catch (Exception e) {
            throw e;
        }
    }

}

