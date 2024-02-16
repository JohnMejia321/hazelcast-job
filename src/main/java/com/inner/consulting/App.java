package com.inner.consulting;
import com.hazelcast.client.config.ClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;



import com.hazelcast.config.Config;
import com.hazelcast.jet.kafka.KafkaSinks;


import org.apache.kafka.common.serialization.StringSerializer;


import java.util.AbstractMap;
import java.util.Properties;
import java.util.UUID;
import java.util.logging.Logger;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class App {





    public static void main(String[] args) {
        // Configuración de propiedades del consumidor
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Para leer desde el inicio

        // Crear el consumidor
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // Suscribirse al tema (topic)
        consumer.subscribe(Collections.singletonList("topic-job"));
        //consumer.subscribe(Collections.singletonList("topic-job"));

        // Suscribirse a los temas (topics) especificados
        //consumer.subscribe(Arrays.asList("topic-pipeline", "topic-job"));


        // Bucle para recibir mensajes
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("Mensaje recibido: %s%n", record.value());
                ejecutarPipeline(record.value());
            }

        }
    }

    private static void ejecutarPipeline(String ocrResult) {
        try {
            Pipeline pipeline = Pipeline.create();
            BatchStage<AbstractMap.SimpleEntry<String, String>> jsonEntries = pipeline
                    .readFrom(Sources.<String>list("sourceList"))
                    .map(entry -> {
                        String[] parts = entry.split("\n");
                        StringBuilder json = new StringBuilder("{");
                        for (String part : parts) {
                            String[] keyValue = part.split(":");
                            if (keyValue.length == 2) {
                                String key = keyValue[0].trim();
                                String value = keyValue[1].trim();
                                json.append(String.format("\"%s\":\"%s\",", key, value));
                            }
                        }
                        if (json.charAt(json.length() - 1) == ',') {
                            json.deleteCharAt(json.length() - 1);
                        }
                       // UUID messageIdJson = empleadorId;
                        UUID messageIdJson = UUID.randomUUID();
                        System.out.print(messageIdJson);
                        json.append(String.format(",\"Id solicitud\":\"%s\"", messageIdJson.toString()));
                        json.append("}");
                        String messageId = messageIdJson.toString();
                        return new AbstractMap.SimpleEntry<>(messageId, json.toString());
                    })
                    .setName("Map String to JSON Object")
                    .setLocalParallelism(1);

            Properties kafkaProps = new Properties();
            kafkaProps.put("bootstrap.servers", "localhost:9092");
            kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            jsonEntries.writeTo(KafkaSinks.kafka(kafkaProps,
                    "my_topic"
            ));
            jsonEntries.writeTo(Sinks.observable("results"));
            jsonEntries.writeTo(Sinks.logger());
            jsonEntries.writeTo(Sinks.map("jsonMap"));



            HazelcastInstance hz = Hazelcast.bootstrappedInstance();


            hz.getList("sourceList").clear(); // Limpiar lista
            hz.getList("sourceList").add(ocrResult); // Agregar elemento a la lista

            Config jobConfig = new Config();
            //jobConfig.getJetConfig().setResourceUploadEnabled(true);
            jobConfig.getJetConfig().setEnabled(true);

            //jobConfig.addClass(App.class);
            hz.getJet().newJob(pipeline);

        } catch (Exception e) {
            Logger.getLogger(App.class.getName()).severe("Error al ejecutar el pipeline: " + e.getMessage());
            throw e;
        }
    }


}
