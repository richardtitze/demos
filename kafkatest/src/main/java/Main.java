import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static java.util.Collections.singleton;

public class Main {
    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private static final String TOPIC_NAME = "test";

    public static void main(String [] args) throws Exception {
        CompletableFuture<Void> consumer = consumeMessages(3);
        Map<String, Object> conf = new HashMap<>();
        conf.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        conf.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        conf.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        conf.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        Producer<String,String> producer = new KafkaProducer<>(conf);
        List<CompletableFuture> futures = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            final int recordNumber = i;
            ProducerRecord<String,String> record = new ProducerRecord<>(TOPIC_NAME, 0, Integer.toString(i), Integer.toString(i));
            CompletableFuture<RecordMetadata> cf = new CompletableFuture<>();
            producer.send(record, ((metadata, exception) -> {
                if (null != exception) {
                    System.out.println(Thread.currentThread().getId() + ":Record " + recordNumber + " sent with error " + exception.getMessage());
                    cf.completeExceptionally(exception);
                } else {
                    System.out.println(Thread.currentThread().getId() + ":Record " + recordNumber + " sent successfully");
                    cf.complete(metadata);
                }
            }));
            futures.add(cf);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
        producer.close();
        consumer.get();
    }

    static CompletableFuture<Void> consumeMessages(int retries) throws Exception {
        return CompletableFuture.runAsync(() -> {
            Map<String, Object> configuration = new HashMap<>();
            configuration.put(ConsumerConfig.GROUP_ID_CONFIG, "a");
            configuration.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
            configuration.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            configuration.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            try (Consumer<String,String> consumer = new KafkaConsumer<>(configuration)) {
                // assign assigns a particular partition of a topic to the consumer
                consumer.assign(singleton(new TopicPartition(TOPIC_NAME, 0)));
                // only this way seeking works
                consumer.seekToBeginning(singleton(new TopicPartition(TOPIC_NAME, 0)));

                //consumer.subscribe(singleton(TOPIC_NAME));
                int tries = retries;
                do {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(2000));
                    if (records.isEmpty()) {
                        System.out.println("Found no records");
                        tries--;
                        continue;
                    }
                    tries = retries;
                    for (ConsumerRecord record : records) {
                        System.out.println("Received: " + record.value() + " with offset " + record.offset());
                    }
                    consumer.commitSync();
                } while (tries > 0);
            }
        });
    }
}
