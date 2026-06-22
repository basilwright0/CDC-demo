package com.example.streams;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Files;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * End-to-end integration test: runs the real denormalization topology against an ephemeral
 * Kafka broker (Testcontainers) with a mock Schema Registry, produces Debezium-shaped Avro
 * records to the input topics, and asserts the enriched record on the real output topic.
 *
 * Unlike the TopologyTestDriver unit test, this exercises real brokers, repartition/changelog
 * topics, network serialization, and Avro over the wire. Run with: gradle integrationTest
 */
@Testcontainers
class PipelineIntegrationTest {

    @Container
    static final ConfluentKafkaContainer KAFKA =
            new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:8.2.2"));

    static final String SR_URL = "mock://pipeline-it";
    static final Map<String, String> SR = Map.of("schema.registry.url", SR_URL);

    static final Schema KEY = parse("{\"type\":\"record\",\"name\":\"K\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}");
    static final Schema ORDERS = parse("""
        {"type":"record","name":"O","fields":[
          {"name":"order_id","type":"string"},{"name":"customer_id","type":"string"},
          {"name":"order_status","type":["null","string"]},
          {"name":"order_purchase_timestamp","type":["null","long"]},
          {"name":"order_approved_at","type":["null","long"]},
          {"name":"order_delivered_carrier_date","type":["null","long"]},
          {"name":"order_delivered_customer_date","type":["null","long"]},
          {"name":"order_estimated_delivery_date","type":["null","long"]}]}""");
    static final Schema CUSTOMERS = parse("""
        {"type":"record","name":"C","fields":[
          {"name":"customer_id","type":"string"},{"name":"customer_unique_id","type":"string"},
          {"name":"customer_zip_code_prefix","type":["null","string"]},
          {"name":"customer_city","type":["null","string"]},{"name":"customer_state","type":["null","string"]}]}""");
    static final Schema PRODUCTS = parse("""
        {"type":"record","name":"P","fields":[
          {"name":"product_id","type":"string"},{"name":"product_category_name","type":["null","string"]}]}""");
    static final Schema TRANSLATION = parse("""
        {"type":"record","name":"T","fields":[
          {"name":"product_category_name","type":"string"},
          {"name":"product_category_name_english","type":["null","string"]}]}""");
    static final Schema ITEMS = parse("""
        {"type":"record","name":"I","fields":[
          {"name":"order_id","type":"string"},{"name":"order_item_id","type":"int"},
          {"name":"product_id","type":"string"},{"name":"seller_id","type":"string"},
          {"name":"price","type":["null","double"]},{"name":"freight_value","type":["null","double"]}]}""");
    static final Schema PAYMENTS = parse("""
        {"type":"record","name":"Pay","fields":[
          {"name":"order_id","type":"string"},{"name":"payment_sequential","type":"int"},
          {"name":"payment_type","type":["null","string"]},{"name":"payment_installments","type":["null","int"]},
          {"name":"payment_value","type":["null","double"]}]}""");
    static final Schema REVIEWS = parse("""
        {"type":"record","name":"R","fields":[
          {"name":"review_pk","type":"long"},{"name":"review_id","type":"string"},
          {"name":"order_id","type":"string"},{"name":"review_score","type":["null","int"]}]}""");

    @Test
    @Timeout(value = 4, unit = TimeUnit.MINUTES)
    void enrichedRowFlowsThroughRealKafka() throws Exception {
        String bootstrap = KAFKA.getBootstrapServers();

        createTopics(bootstrap,
                DenormalizationTopology.ORDERS, DenormalizationTopology.CUSTOMERS,
                DenormalizationTopology.PRODUCTS, DenormalizationTopology.TRANSLATION,
                DenormalizationTopology.ITEMS, DenormalizationTopology.PAYMENTS,
                DenormalizationTopology.REVIEWS, DenormalizationTopology.OUTPUT);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "pipeline-it-" + UUID.randomUUID());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(StreamsConfig.STATE_DIR_CONFIG, Files.createTempDirectory("kstest").toString());
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 500);
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);

        KafkaStreams streams = new KafkaStreams(DenormalizationTopology.build(SR), props);
        try {
            streams.start();
            awaitRunning(streams);

            try (KafkaProducer<GenericRecord, GenericRecord> p = producer(bootstrap)) {
                send(p, DenormalizationTopology.CUSTOMERS, key("c1"),
                        rec(CUSTOMERS, "customer_id", "c1", "customer_unique_id", "u1",
                                "customer_zip_code_prefix", "01000", "customer_city", "sao paulo", "customer_state", "SP"));
                send(p, DenormalizationTopology.TRANSLATION, key("perfumaria"),
                        rec(TRANSLATION, "product_category_name", "perfumaria", "product_category_name_english", "perfumery"));
                send(p, DenormalizationTopology.TRANSLATION, key("informatica_acessorios"),
                        rec(TRANSLATION, "product_category_name", "informatica_acessorios", "product_category_name_english", "computers_accessories"));
                send(p, DenormalizationTopology.PRODUCTS, key("p1"),
                        rec(PRODUCTS, "product_id", "p1", "product_category_name", "perfumaria"));
                send(p, DenormalizationTopology.PRODUCTS, key("p2"),
                        rec(PRODUCTS, "product_id", "p2", "product_category_name", "informatica_acessorios"));
                send(p, DenormalizationTopology.ORDERS, key("o1"),
                        rec(ORDERS, "order_id", "o1", "customer_id", "c1", "order_status", "delivered",
                                "order_purchase_timestamp", 1_000L, "order_approved_at", 2_000L,
                                "order_delivered_carrier_date", 3_000L, "order_delivered_customer_date", 4_000L,
                                "order_estimated_delivery_date", 5_000L));
                send(p, DenormalizationTopology.ITEMS, key("o1#1"),
                        rec(ITEMS, "order_id", "o1", "order_item_id", 1, "product_id", "p1", "seller_id", "s1",
                                "price", 100.0, "freight_value", 10.0));
                send(p, DenormalizationTopology.ITEMS, key("o1#2"),
                        rec(ITEMS, "order_id", "o1", "order_item_id", 2, "product_id", "p2", "seller_id", "s2",
                                "price", 50.0, "freight_value", 5.0));
                send(p, DenormalizationTopology.PAYMENTS, key("o1#1"),
                        rec(PAYMENTS, "order_id", "o1", "payment_sequential", 1, "payment_type", "credit_card",
                                "payment_installments", 3, "payment_value", 165.0));
                send(p, DenormalizationTopology.REVIEWS, key("1"),
                        rec(REVIEWS, "review_pk", 1L, "review_id", "r1", "order_id", "o1", "review_score", 5));
                p.flush();
            }

            GenericRecord e = awaitEnriched("o1");
            assertNotNull(e, "expected an enriched record for o1");
            assertEquals("sao paulo", str(e, "customer_city"));
            assertEquals("delivered", str(e, "order_status"));
            assertEquals(2, e.get("item_count"));
            assertEquals(150.0, (double) e.get("total_price"), 0.001);
            assertEquals(2, e.get("distinct_seller_count"));
            assertEquals("computers_accessories, perfumery", str(e, "product_categories"));
            assertEquals("credit_card", str(e, "payment_types"));
            assertEquals(165.0, (double) e.get("total_payment_value"), 0.001);
            assertEquals(3, e.get("max_installments"));
            assertEquals(5.0, (double) e.get("review_avg_score"), 0.001);
        } finally {
            streams.close(Duration.ofSeconds(20));
        }
    }

    private GenericRecord awaitEnriched(String orderId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "it-consumer-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put("schema.registry.url", SR_URL);

        GenericRecord latest = null;
        long deadline = System.currentTimeMillis() + Duration.ofMinutes(2).toMillis();
        try (KafkaConsumer<String, GenericRecord> c = new KafkaConsumer<>(props)) {
            c.subscribe(List.of(DenormalizationTopology.OUTPUT));
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, GenericRecord> records = c.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, GenericRecord> r : records) {
                    if (orderId.equals(r.key())) latest = r.value();
                }
                if (latest != null && Integer.valueOf(2).equals(latest.get("item_count"))
                        && latest.get("review_avg_score") != null) {
                    return latest;
                }
            }
        }
        return latest;
    }

    private static KafkaProducer<GenericRecord, GenericRecord> producer(String bootstrap) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put("schema.registry.url", SR_URL);
        return new KafkaProducer<>(props);
    }

    private static void send(KafkaProducer<GenericRecord, GenericRecord> p, String topic, GenericRecord k, GenericRecord v) {
        p.send(new ProducerRecord<>(topic, k, v));
    }

    private static void createTopics(String bootstrap, String... names) throws Exception {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrap))) {
            admin.createTopics(java.util.Arrays.stream(names)
                    .map(n -> new NewTopic(n, 1, (short) 1)).toList()).all().get(30, TimeUnit.SECONDS);
        }
    }

    private static void awaitRunning(KafkaStreams streams) throws InterruptedException {
        long deadline = System.currentTimeMillis() + Duration.ofMinutes(1).toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (streams.state() == KafkaStreams.State.RUNNING) return;
            Thread.sleep(200);
        }
    }

    private static GenericRecord key(String id) {
        return rec(KEY, "id", id);
    }

    private static GenericRecord rec(Schema schema, Object... kv) {
        GenericRecord r = new GenericData.Record(schema);
        for (int i = 0; i < kv.length; i += 2) r.put((String) kv[i], kv[i + 1]);
        return r;
    }

    private static String str(GenericRecord r, String field) {
        Object o = r.get(field);
        return o == null ? null : o.toString();
    }

    private static Schema parse(String json) {
        return new Schema.Parser().parse(json);
    }
}
