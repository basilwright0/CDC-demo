package com.example.streams;

import org.apache.avro.generic.GenericRecord;

import static com.example.streams.Avro.d;
import static com.example.streams.Avro.i;
import static com.example.streams.Avro.l;
import static com.example.streams.Avro.s;

/** Plain value objects parsed from the input topics (mutable, JSON-serializable). */
final class Domain {

    static final class Order {
        public String orderId, customerId, status;
        public Long purchaseTs, approvedTs, carrierTs, deliveredTs, estimatedTs;

        public Order() {}

        static Order from(GenericRecord r) {
            Order o = new Order();
            o.orderId = s(r, "order_id");
            o.customerId = s(r, "customer_id");
            o.status = s(r, "order_status");
            o.purchaseTs = l(r, "order_purchase_timestamp");
            o.approvedTs = l(r, "order_approved_at");
            o.carrierTs = l(r, "order_delivered_carrier_date");
            o.deliveredTs = l(r, "order_delivered_customer_date");
            o.estimatedTs = l(r, "order_estimated_delivery_date");
            return o;
        }
    }

    static final class Customer {
        public String customerId, uniqueId, zip, city, state;

        public Customer() {}

        static Customer from(GenericRecord r) {
            Customer c = new Customer();
            c.customerId = s(r, "customer_id");
            c.uniqueId = s(r, "customer_unique_id");
            c.zip = s(r, "customer_zip_code_prefix");
            c.city = s(r, "customer_city");
            c.state = s(r, "customer_state");
            return c;
        }
    }

    static final class Product {
        public String productId, categoryName;

        public Product() {}

        static Product from(GenericRecord r) {
            Product p = new Product();
            p.productId = s(r, "product_id");
            p.categoryName = s(r, "product_category_name");
            return p;
        }
    }

    /** Product with its (English, when available) category resolved. */
    static final class ProductCat {
        public String productId, category;

        public ProductCat() {}

        public ProductCat(String productId, String category) {
            this.productId = productId;
            this.category = category;
        }
    }

    static final class Item {
        public String orderId, productId, sellerId;
        public Integer itemId;
        public Double price, freight;

        public Item() {}

        static Item from(GenericRecord r) {
            Item it = new Item();
            it.orderId = s(r, "order_id");
            it.itemId = i(r, "order_item_id");
            it.productId = s(r, "product_id");
            it.sellerId = s(r, "seller_id");
            it.price = d(r, "price");
            it.freight = d(r, "freight_value");
            return it;
        }
    }

    /** Item with its product category attached, ready to aggregate by order. */
    static final class ItemEnriched {
        public String orderId, sellerId, category;
        public Double price, freight;

        public ItemEnriched() {}

        public ItemEnriched(String orderId, String sellerId, Double price, Double freight, String category) {
            this.orderId = orderId;
            this.sellerId = sellerId;
            this.price = price;
            this.freight = freight;
            this.category = category;
        }
    }

    static final class Payment {
        public String orderId, type;
        public Integer sequential, installments;
        public Double value;

        public Payment() {}

        static Payment from(GenericRecord r) {
            Payment p = new Payment();
            p.orderId = s(r, "order_id");
            p.sequential = i(r, "payment_sequential");
            p.type = s(r, "payment_type");
            p.installments = i(r, "payment_installments");
            p.value = d(r, "payment_value");
            return p;
        }
    }

    static final class Review {
        public String reviewId, orderId;
        public Integer score;

        public Review() {}

        static Review from(GenericRecord r) {
            Review v = new Review();
            v.reviewId = s(r, "review_id");
            v.orderId = s(r, "order_id");
            v.score = i(r, "review_score");
            return v;
        }
    }

    private Domain() {}
}
