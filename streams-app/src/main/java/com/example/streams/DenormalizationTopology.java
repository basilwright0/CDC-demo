package com.example.streams;

import com.example.streams.Aggregates.ItemAgg;
import com.example.streams.Aggregates.PaymentAgg;
import com.example.streams.Aggregates.ReviewAgg;
import com.example.streams.Domain.Customer;
import com.example.streams.Domain.Item;
import com.example.streams.Domain.ItemEnriched;
import com.example.streams.Domain.Order;
import com.example.streams.Domain.Payment;
import com.example.streams.Domain.Product;
import com.example.streams.Domain.ProductCat;
import com.example.streams.Domain.Review;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * Denormalizes the eight Olist CDC topics into a flat {@code order_enriched} topic:
 * orders ⨝ customers (FK), plus per-order aggregates of items (with product category,
 * resolved via products ⨝ category-translation), payments, and reviews.
 */
final class DenormalizationTopology {

    static final String PREFIX = "mysql.olist.";
    static final String ORDERS = PREFIX + "orders";
    static final String CUSTOMERS = PREFIX + "customers";
    static final String PRODUCTS = PREFIX + "products";
    static final String TRANSLATION = PREFIX + "product_category_name_translation";
    static final String ITEMS = PREFIX + "order_items";
    static final String PAYMENTS = PREFIX + "order_payments";
    static final String REVIEWS = PREFIX + "order_reviews";
    static final String OUTPUT = "order_enriched";

    static final Schema OUTPUT_SCHEMA = loadSchema();

    static Topology build(Map<String, String> serdeConfig) {
        StreamsBuilder b = new StreamsBuilder();

        Serde<GenericRecord> avroKey = Avro.serde(serdeConfig, true);
        Serde<GenericRecord> avroVal = Avro.serde(serdeConfig, false);
        Serde<GenericRecord> avroOut = Avro.serde(serdeConfig, false);
        Serde<String> str = Serdes.String();
        Consumed<GenericRecord, GenericRecord> consumed = Consumed.with(avroKey, avroVal);

        Serde<Order> orderS = new JsonSerde<>(Order.class);
        Serde<Customer> custS = new JsonSerde<>(Customer.class);
        Serde<Product> prodS = new JsonSerde<>(Product.class);
        Serde<ProductCat> pcatS = new JsonSerde<>(ProductCat.class);
        Serde<Item> itemS = new JsonSerde<>(Item.class);
        Serde<ItemEnriched> ieS = new JsonSerde<>(ItemEnriched.class);
        Serde<Payment> payS = new JsonSerde<>(Payment.class);
        Serde<Review> revS = new JsonSerde<>(Review.class);
        Serde<ItemAgg> iaS = new JsonSerde<>(ItemAgg.class);
        Serde<PaymentAgg> paS = new JsonSerde<>(PaymentAgg.class);
        Serde<ReviewAgg> raS = new JsonSerde<>(ReviewAgg.class);
        Serde<Enriched> enrS = new JsonSerde<>(Enriched.class);

        // --- dimension tables (re-keyed to their business key) ---
        KTable<String, Order> orders = b.stream(ORDERS, consumed)
                .map((k, v) -> KeyValue.pair(Avro.s(v, "order_id"), Order.from(v)))
                .toTable(Named.as("orders-table"), Materialized.with(str, orderS));

        KTable<String, Customer> customers = b.stream(CUSTOMERS, consumed)
                .map((k, v) -> KeyValue.pair(Avro.s(v, "customer_id"), Customer.from(v)))
                .toTable(Named.as("customers-table"), Materialized.with(str, custS));

        KTable<String, Product> products = b.stream(PRODUCTS, consumed)
                .map((k, v) -> KeyValue.pair(Avro.s(v, "product_id"), Product.from(v)))
                .toTable(Named.as("products-table"), Materialized.with(str, prodS));

        KTable<String, String> translation = b.stream(TRANSLATION, consumed)
                .map((k, v) -> KeyValue.pair(Avro.s(v, "product_category_name"),
                        Avro.s(v, "product_category_name_english")))
                .toTable(Named.as("translation-table"), Materialized.with(str, Serdes.String()));

        // products ⨝ translation (FK on category name) → English category, falling back to original
        KTable<String, ProductCat> productCat = products.leftJoin(translation,
                p -> p.categoryName,
                (p, english) -> new ProductCat(p.productId, english != null ? english : p.categoryName),
                Materialized.with(str, pcatS));

        // --- items: keyed by composite PK, enriched with category, aggregated by order ---
        KTable<String, Item> items = b.stream(ITEMS, consumed)
                .map((k, v) -> KeyValue.pair(Avro.s(v, "order_id") + "#" + Avro.i(v, "order_item_id"), Item.from(v)))
                .toTable(Named.as("items-table"), Materialized.with(str, itemS));

        KTable<String, ItemEnriched> itemEnriched = items.leftJoin(productCat,
                it -> it.productId,
                (it, pc) -> new ItemEnriched(it.orderId, it.sellerId, it.price, it.freight,
                        pc != null ? pc.category : null),
                Materialized.with(str, ieS));

        KTable<String, ItemAgg> itemAgg = itemEnriched
                .groupBy((k, ie) -> KeyValue.pair(ie.orderId, ie), Grouped.with(str, ieS))
                .aggregate(ItemAgg::new, (o, ie, agg) -> agg.add(ie), (o, ie, agg) -> agg.remove(ie),
                        Materialized.with(str, iaS));

        // --- payments aggregated by order ---
        KTable<String, Payment> payments = b.stream(PAYMENTS, consumed)
                .map((k, v) -> KeyValue.pair(Avro.s(v, "order_id") + "#" + Avro.i(v, "payment_sequential"), Payment.from(v)))
                .toTable(Named.as("payments-table"), Materialized.with(str, payS));

        KTable<String, PaymentAgg> paymentAgg = payments
                .groupBy((k, p) -> KeyValue.pair(p.orderId, p), Grouped.with(str, payS))
                .aggregate(PaymentAgg::new, (o, p, agg) -> agg.add(p), (o, p, agg) -> agg.remove(p),
                        Materialized.with(str, paS));

        // --- reviews (keyed by surrogate PK) aggregated by order ---
        KTable<String, Review> reviews = b.stream(REVIEWS, consumed)
                .map((k, v) -> KeyValue.pair(Avro.s(v, "review_pk"), Review.from(v)))
                .toTable(Named.as("reviews-table"), Materialized.with(str, revS));

        KTable<String, ReviewAgg> reviewAgg = reviews
                .groupBy((k, r) -> KeyValue.pair(r.orderId, r), Grouped.with(str, revS))
                .aggregate(ReviewAgg::new, (o, r, agg) -> agg.add(r), (o, r, agg) -> agg.remove(r),
                        Materialized.with(str, raS));

        // --- assemble: orders ⨝ customers (FK), then equi-join the per-order aggregates ---
        KTable<String, Enriched> enriched = orders
                .leftJoin(customers, o -> o.customerId, Enriched::new, Materialized.with(str, enrS))
                .leftJoin(itemAgg, Enriched::withItems, Materialized.with(str, enrS))
                .leftJoin(paymentAgg, Enriched::withPayments, Materialized.with(str, enrS))
                .leftJoin(reviewAgg, Enriched::withReviews, Materialized.with(str, enrS));

        enriched.toStream()
                .mapValues(e -> e.toAvro(OUTPUT_SCHEMA))
                .to(OUTPUT, Produced.with(str, avroOut));

        return b.build();
    }

    private static Schema loadSchema() {
        try (InputStream in = DenormalizationTopology.class.getResourceAsStream("/order_enriched.avsc")) {
            if (in == null) throw new IllegalStateException("order_enriched.avsc not found on classpath");
            return new Schema.Parser().parse(in);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private DenormalizationTopology() {}
}
