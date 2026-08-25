package com.example.hexagonal_completed_design.order.infrastructure.config;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaConfig {

    // 1. L'usine à Producer (Envoi)
    /*acks=0 : Le producteur tire le message et n'attend aucune réponse. (Très rapide, mais si Kafka est éteint, le message est perdu).
      acks=1 : Le broker "Leader" dit qu'il l'a écrit sur son disque. (Rapide, mais si le Leader brûle 1 seconde après, le message est perdu).
      acks=all (ou -1) : Le Leader écrit le message, attend que ses "clones" (les ISR - In-Sync Replicas) l'écrivent aussi, et ENSUITE il répond au producteur. (Sécurité maximale ! C'est ce qu'utilisent les banques).*/
    @Bean
    public ProducerFactory<Object, Object> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // 🌟 ÉTAPE 1 : LA SÉCURITÉ DES DONNÉES (ACKS)

        // "all" signifie que le leader ET les répliques doivent confirmer la réception
        configProps.put(ProducerConfig.ACKS_CONFIG, "all");

        // Si ça échoue, Kafka réessaiera tout seul d'envoyer le message
        configProps.put(ProducerConfig.RETRIES_CONFIG, 3);

        // Indispensable avec les retries pour éviter que Kafka n'envoie le message en double en cas de micro-coupure réseau
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        return new DefaultKafkaProducerFactory<>(configProps);
    }

    // 2. Le KafkaTemplate pour le Publisher
    @Bean
    public KafkaTemplate<Object, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    // 3. L'ErrorHandler pour la DLQ (3 essais, puis quarantaine)
    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<Object, Object> template) {
        // 🌟 NOUVEAU : On prend le contrôle total du routage vers la DLQ
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template,
                (record, exception) -> {
                    System.out.println("☠️ ÉCHEC DÉFINITIF (4 essais) : Déplacement du message vers la DLQ...");
                    // On force le nom ".DLQ" et le "-1" laisse Kafka gérer la partition tout seul
                    return new TopicPartition(record.topic() + ".DLQ", -1);
                });

        FixedBackOff backOff = new FixedBackOff(1000L, 3); // 3 retries
        return new DefaultErrorHandler(recoverer, backOff);
    }

    // 4. L'usine à Consumer (Réception) -> TOUT EN STRING !
    @Bean
    public ConsumerFactory<Object, Object> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    // 5. NOUVEAU : Le convertisseur intelligent qui transforme le String JSON en Objet Java
    /*@Bean
    public RecordMessageConverter converter() {
        return new JacksonJsonMessageConverter();
    }*/
    @Bean
    public NewTopic orderEventsTopic() {
        return TopicBuilder.name("order-events-secure")
                .partitions(3) // 3 files d'attente parallèles pour la performance
                .replicas(3)   // 🌟 3 copies du message sur 3 serveurs différents
                .config("min.insync.replicas", "2") // 🌟 Sécurité ISR : 2 brokers minimum doivent confirmer l'écriture pour le acks=all
                .build();
    }

    // 6. On assemble tout pour le @KafkaListener
    @Bean
    public ConcurrentKafkaListenerContainerFactory<Object, Object> kafkaListenerContainerFactory(
            ConsumerFactory<Object, Object> consumerFactory,
            DefaultErrorHandler errorHandler) {

        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(errorHandler); // Active la DLQ

        return factory;
    }

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        return new KafkaAdmin(configs);
    }
}