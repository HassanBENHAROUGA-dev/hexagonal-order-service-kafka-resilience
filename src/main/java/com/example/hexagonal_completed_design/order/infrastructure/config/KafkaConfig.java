package com.example.hexagonal_completed_design.order.infrastructure.config;

import com.example.hexagonal_completed_design.order.infrastructure.exception.MissingEventIdException;
import lombok.extern.slf4j.Slf4j;
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
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Configuration
@EnableKafka
public class KafkaConfig {

    private static final String BOOTSTRAP_SERVERS =
            "localhost:9092,localhost:9093,localhost:9094";

    private static final String ORDER_EVENTS_TOPIC =
            "order-events-secure";

    private static final String ORDER_EVENTS_DLT =
            "order-events-secure.DLT";

    private static final int PARTITION_COUNT = 3;
    private static final int REPLICATION_FACTOR = 3;
    private static final String MIN_IN_SYNC_REPLICAS = "2";

    private static final long RETRY_INTERVAL_MS = 1000L;
    private static final long RETRY_ATTEMPTS = 3L;

    @Bean
    public ProducerFactory<Object, Object> producerFactory() {

        Map<String, Object> configProps = new HashMap<>();

        configProps.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS
        );

        configProps.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class
        );

        configProps.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class
        );

        /*
         * With acks=all, the producer waits for the broker to satisfy
         * the topic's durability requirements before acknowledging
         * a successful write.
         *
         * Combined with min.insync.replicas=2, an event cannot be
         * successfully published when fewer than two replicas are in sync.
         */
        configProps.put(
                ProducerConfig.ACKS_CONFIG,
                "all"
        );

        /*
         * Producer retries protect against transient publication failures.
         *
         * Producer idempotence prevents duplicate records caused by
         * retries performed within the same producer session.
         *
         * It does not replace consumer-side idempotency, since an Outbox
         * event may be published again after an application restart.
         */
        configProps.put(
                ProducerConfig.RETRIES_CONFIG,
                3
        );

        configProps.put(
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
                true
        );

        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<Object, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    /*
     * Handles consumer processing failures.
     *
     * Retryable failures:
     * initial attempt + 3 retries, with a 1-second delay between retries.
     *
     * After the retries are exhausted, the record is published to the DLT.
     *
     * Structural errors such as a missing eventId are non-retryable and
     * are routed directly to the DLT.
     */
    @Bean
    public DefaultErrorHandler errorHandler(
            KafkaTemplate<Object, Object> kafkaTemplate
    ) {

        DeadLetterPublishingRecoverer recoverer =
                new DeadLetterPublishingRecoverer(
                        kafkaTemplate,
                        (record, exception) -> {

                            log.error(
                                    "☠️ KAFKA PROCESSING FAILED - ROUTING TO DLT | topic={} | partition={} | offset={} | exception={}",
                                    record.topic(),
                                    record.partition(),
                                    record.offset(),
                                    exception.getClass().getSimpleName()
                            );

                            return new TopicPartition(
                                    record.topic() + ".DLT",
                                    -1
                            );
                        }
                );

        FixedBackOff backOff = new FixedBackOff(
                RETRY_INTERVAL_MS,
                RETRY_ATTEMPTS
        );

        DefaultErrorHandler errorHandler =
                new DefaultErrorHandler(recoverer, backOff);

        /*
         * A missing eventId is a structural message error.
         * Retrying the same record cannot repair the missing header,
         * so the record is sent directly to the DLT.
         */
        errorHandler.addNotRetryableExceptions(
                MissingEventIdException.class
        );

        return errorHandler;
    }

    @Bean
    public ConsumerFactory<Object, Object> consumerFactory() {

        Map<String, Object> props = new HashMap<>();

        props.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS
        );

        props.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class
        );

        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class
        );

        /*
         * Topics are created explicitly through KafkaAdmin and NewTopic
         * beans so that partitioning and replication settings are controlled
         * by the application instead of broker defaults.
         */
        props.put(
                ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG,
                false
        );

        return new DefaultKafkaConsumerFactory<>(props);
    }

    /*
     * Main domain-event topic.
     *
     * Three partitions provide parallelism, while a replication factor
     * of three stores each partition replica across the three brokers.
     *
     * min.insync.replicas=2 requires at least two synchronized replicas
     * for successful writes when the producer uses acks=all.
     */
    @Bean
    public NewTopic orderEventsTopic() {
        return TopicBuilder.name(ORDER_EVENTS_TOPIC)
                .partitions(PARTITION_COUNT)
                .replicas(REPLICATION_FACTOR)
                .config(
                        "min.insync.replicas",
                        MIN_IN_SYNC_REPLICAS
                )
                .build();
    }

    /*
     * Dead Letter Topic used to isolate records that cannot be processed
     * successfully by the consumer.
     *
     * It uses the same replication and durability settings as the
     * main order-events topic.
     */
    @Bean
    public NewTopic orderEventsDltTopic() {
        return TopicBuilder.name(ORDER_EVENTS_DLT)
                .partitions(PARTITION_COUNT)
                .replicas(REPLICATION_FACTOR)
                .config(
                        "min.insync.replicas",
                        MIN_IN_SYNC_REPLICAS
                )
                .build();
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<Object, Object>
    kafkaListenerContainerFactory(
            ConsumerFactory<Object, Object> consumerFactory,
            DefaultErrorHandler errorHandler
    ) {

        ConcurrentKafkaListenerContainerFactory<Object, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(errorHandler);

        return factory;
    }

    /*
     * KafkaAdmin creates and manages the topics declared through
     * the NewTopic beans when the application starts.
     */
    @Bean
    public KafkaAdmin kafkaAdmin() {

        Map<String, Object> configs = new HashMap<>();

        configs.put(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS
        );

        return new KafkaAdmin(configs);
    }
}