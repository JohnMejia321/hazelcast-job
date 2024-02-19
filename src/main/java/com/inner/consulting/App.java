package com.inner.consulting;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.map.IMap;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.kafka.KafkaSinks;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.AbstractMap;
import java.util.Properties;
import java.util.logging.Logger;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;

public class App {
    private static String pipelineMessage = null;
    private static String jobMessage = null;

    public static void main(String[] args) {
        // Configuración de propiedades del consumidor Kafka
        Properties kafkaProps = new Properties();
        kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "my-consumer-group");
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Para leer desde el inicio

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaProps);
        consumer.subscribe(Arrays.asList("topic-pipeline", "topic-job"));

        ClientConfig clientConfig = new ClientConfig();
        HazelcastInstance hazelcastInstance = HazelcastClient.newHazelcastClient(clientConfig);
        IMap<String, String> hazelcastMap = hazelcastInstance.getMap("myMap");

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Mensaje recibido de topic %s: %s%n", record.topic(), record.value());

                    String currentMessage = record.value();

                    // Asegúrate de que los mensajes se estén procesando correctamente
                    if (record.topic().equals("topic-pipeline")) {
                        pipelineMessage = currentMessage;
                    } else if (record.topic().equals("topic-job")) {
                        jobMessage = currentMessage;
                    }

                    if (pipelineMessage != null && jobMessage != null) {
                        try {
                            // Ejecutar el pipeline para enviar a Kafka
                            ejecutarPipeline(pipelineMessage, jobMessage);
                            hazelcastMap.put(jobMessage, pipelineMessage);
                            System.out.println("Mensaje agregado al mapa de Hazelcast: " + currentMessage);
                        } catch (Exception e) {
                            // Manejar cualquier excepción que pueda ocurrir al agregar mensajes al mapa
                            System.err.println("Error al agregar mensaje al mapa de Hazelcast: " + e.getMessage());
                            e.printStackTrace();
                        }
                        // Reiniciar los mensajes
                        pipelineMessage = null;
                        jobMessage = null;
                    }
                }
            }
        } finally {
            consumer.close();
        }
    }

    private static void ejecutarPipeline(String pipelineMsg, String jobId) {
        try {
            Pipeline pipeline = Pipeline.create();
            pipeline.readFrom(Sources.<String>list("sourceList"))
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
                        String messageIdJson = jobId;
                        json.append(String.format(",\"Id solicitud\":\"%s\"", messageIdJson));
                        json.append("}");
                        return new AbstractMap.SimpleEntry<>(messageIdJson, json.toString());
                    })
                    .setName("Map String to JSON Object")
                    .setLocalParallelism(1)
                    .writeTo(KafkaSinks.kafka(producerProps(), "my_topic"));
            HazelcastInstance hazelcastInstance = Hazelcast.bootstrappedInstance();
            hazelcastInstance.getList("sourceList").clear(); // Limpiar lista
            hazelcastInstance.getList("sourceList").add(pipelineMsg); // Agregar elemento a la lista

            hazelcastInstance.getJet().newJob(pipeline);

        } catch (Exception e) {
            Logger.getLogger(App.class.getName()).severe("Error al ejecutar el pipeline: " + e.getMessage());
            throw e;
        }
    }

    private static Properties producerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }
}
