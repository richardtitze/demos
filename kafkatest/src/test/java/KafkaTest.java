import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singleton;
import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
public class KafkaTest {

    @Container
    private static KafkaContainer kafkaContainer = new KafkaContainer("5.2.1");

    static final short replicationFactor = 1;
    static final int numPartitions = 1;

    @BeforeEach
    void beforeEach() throws Exception {
        createTopic("test");
    }

    @AfterEach
    void afterEach() throws Exception {
        deleteTopic("test");
    }

    static void createTopic(String topic) throws Exception {
        try (AdminClient client = adminClient()) {
            client.createTopics(Collections.singleton(new NewTopic(topic,numPartitions,replicationFactor))).all().get();
        }
    }

    static void deleteTopic(String topic) throws Exception {
        try (AdminClient client = adminClient()) {
            client.deleteTopics(Collections.singleton(topic)).all().get();
        }
    }

    static AdminClient adminClient() {
        Map<String,Object> configuration = new HashMap<>();
        configuration.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        return KafkaAdminClient.create(configuration);
    }

    @Test
    void produce_and_then_consume_a_batch_of_messages() throws Exception {
        produceMessages(10);
        Map<String,Object> configuration = new HashMap<>();
        configuration.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        configuration.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configuration.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configuration.put(ConsumerConfig.GROUP_ID_CONFIG, "a");
        configuration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try(Consumer<String,String> consumer = new KafkaConsumer<>(configuration)) {
            consumer.subscribe(singleton("test"));

            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(2000));
            assertThat(records.count()).isEqualTo(10);
        }
    }

    static void produceMessages(int count) throws Exception {
        Map<String,Object> configuration = new HashMap<>();
        configuration.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        configuration.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configuration.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (Producer<String,String> producer = new KafkaProducer<>(configuration)) {
            for (int i = 0; i < count; i++) {
                ProducerRecord record = new ProducerRecord("test", 0,"1key" + i, "value" + 1);
                producer.send(record, (metadata, exception) -> {
                    if (null != exception) {
                        exception.printStackTrace();
                    } else {
                        System.out.println("Sent record");
                    }
                }).get();
            }
        }
    }

    static String bootstrapServers() {
        return kafkaContainer.getBootstrapServers();
//        return "localhost:9092";
    }
}
