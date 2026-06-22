package com.example.streams;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class DenormalizationTopologyTest {

    static final Map<String, String> SR = Map.of("schema.registry.url", "mock://denorm-test");

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

    TopologyTestDriver driver;
    Serde<GenericRecord> keySerde, valSerde, outSerde;

    @BeforeEach
    void setup() throws Exception {
        keySerde = new GenericAvroSerde(); keySerde.configure(SR, true);
        valSerde = new GenericAvroSerde(); valSerde.configure(SR, false);
        outSerde = new GenericAvroSerde(); outSerde.configure(SR, false);

        Topology topology = DenormalizationTopology.build(SR);
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "denorm-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, Files.createTempDirectory("kstest").toString());
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0); // emit every update
        driver = new TopologyTestDriver(topology, props);
    }

    @AfterEach
    void tearDown() {
        if (driver != null) driver.close();
    }

    @Test
    void enrichesAnOrderFromAllSources() {
        in(DenormalizationTopology.CUSTOMERS).pipeInput(key("c1"),
                rec(CUSTOMERS, "customer_id", "c1", "customer_unique_id", "u1",
                        "customer_zip_code_prefix", "01000", "customer_city", "sao paulo", "customer_state", "SP"));
        in(DenormalizationTopology.TRANSLATION).pipeInput(key("perfumaria"),
                rec(TRANSLATION, "product_category_name", "perfumaria", "product_category_name_english", "perfumery"));
        in(DenormalizationTopology.TRANSLATION).pipeInput(key("informatica_acessorios"),
                rec(TRANSLATION, "product_category_name", "informatica_acessorios", "product_category_name_english", "computers_accessories"));
        in(DenormalizationTopology.PRODUCTS).pipeInput(key("p1"),
                rec(PRODUCTS, "product_id", "p1", "product_category_name", "perfumaria"));
        in(DenormalizationTopology.PRODUCTS).pipeInput(key("p2"),
                rec(PRODUCTS, "product_id", "p2", "product_category_name", "informatica_acessorios"));
        in(DenormalizationTopology.ORDERS).pipeInput(key("o1"),
                rec(ORDERS, "order_id", "o1", "customer_id", "c1", "order_status", "delivered",
                        "order_purchase_timestamp", 1_000L, "order_approved_at", 2_000L,
                        "order_delivered_carrier_date", 3_000L, "order_delivered_customer_date", 4_000L,
                        "order_estimated_delivery_date", 5_000L));
        in(DenormalizationTopology.ITEMS).pipeInput(key("o1#1"),
                rec(ITEMS, "order_id", "o1", "order_item_id", 1, "product_id", "p1", "seller_id", "s1",
                        "price", 100.0, "freight_value", 10.0));
        in(DenormalizationTopology.ITEMS).pipeInput(key("o1#2"),
                rec(ITEMS, "order_id", "o1", "order_item_id", 2, "product_id", "p2", "seller_id", "s2",
                        "price", 50.0, "freight_value", 5.0));
        in(DenormalizationTopology.PAYMENTS).pipeInput(key("o1#1"),
                rec(PAYMENTS, "order_id", "o1", "payment_sequential", 1, "payment_type", "credit_card",
                        "payment_installments", 3, "payment_value", 165.0));
        in(DenormalizationTopology.REVIEWS).pipeInput(key("1"),
                rec(REVIEWS, "review_pk", 1L, "review_id", "r1", "order_id", "o1", "review_score", 5));

        TestOutputTopic<String, GenericRecord> out = driver.createOutputTopic(
                DenormalizationTopology.OUTPUT, Serdes.String().deserializer(), outSerde.deserializer());

        Map<String, GenericRecord> latest = new HashMap<>();
        out.readKeyValuesToList().forEach(kv -> latest.put(kv.key, kv.value));

        GenericRecord e = latest.get("o1");
        assertNotNull(e, "expected an enriched record for o1");
        assertEquals("sao paulo", str(e, "customer_city"));
        assertEquals("delivered", str(e, "order_status"));
        assertEquals(2, e.get("item_count"));
        assertEquals(150.0, (double) e.get("total_price"), 0.001);
        assertEquals(15.0, (double) e.get("total_freight"), 0.001);
        assertEquals(2, e.get("distinct_seller_count"));
        assertEquals(2, e.get("distinct_category_count"));
        assertEquals("computers_accessories, perfumery", str(e, "product_categories"));
        assertEquals(1, e.get("payment_count"));
        assertEquals("credit_card", str(e, "payment_types"));
        assertEquals(165.0, (double) e.get("total_payment_value"), 0.001);
        assertEquals(3, e.get("max_installments"));
        assertEquals(1, e.get("review_count"));
        assertEquals(5.0, (double) e.get("review_avg_score"), 0.001);
    }

    private TestInputTopic<GenericRecord, GenericRecord> in(String topic) {
        return driver.createInputTopic(topic, keySerde.serializer(), valSerde.serializer());
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
