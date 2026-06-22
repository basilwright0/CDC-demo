package com.example.streams;

import com.example.streams.Aggregates.ItemAgg;
import com.example.streams.Aggregates.PaymentAgg;
import com.example.streams.Aggregates.ReviewAgg;
import com.example.streams.Domain.Customer;
import com.example.streams.Domain.Order;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/** Accumulates the joined pieces of one order, then renders the flat Avro output row. */
final class Enriched {
    public Order order;
    public Customer customer;
    public ItemAgg items;
    public PaymentAgg payments;
    public ReviewAgg reviews;

    public Enriched() {}

    Enriched(Order order, Customer customer) {
        this.order = order;
        this.customer = customer;
    }

    Enriched withItems(ItemAgg ia) { Enriched e = copy(); e.items = ia; return e; }
    Enriched withPayments(PaymentAgg pa) { Enriched e = copy(); e.payments = pa; return e; }
    Enriched withReviews(ReviewAgg ra) { Enriched e = copy(); e.reviews = ra; return e; }

    private Enriched copy() {
        Enriched e = new Enriched();
        e.order = order;
        e.customer = customer;
        e.items = items;
        e.payments = payments;
        e.reviews = reviews;
        return e;
    }

    GenericRecord toAvro(Schema schema) {
        GenericRecord r = new GenericData.Record(schema);
        r.put("order_id", order.orderId);
        r.put("customer_id", order.customerId);
        r.put("customer_unique_id", customer != null ? customer.uniqueId : null);
        r.put("customer_city", customer != null ? customer.city : null);
        r.put("customer_state", customer != null ? customer.state : null);
        r.put("order_status", order.status);
        r.put("order_purchase_timestamp", order.purchaseTs);
        r.put("order_approved_at", order.approvedTs);
        r.put("order_delivered_carrier_date", order.carrierTs);
        r.put("order_delivered_customer_date", order.deliveredTs);
        r.put("order_estimated_delivery_date", order.estimatedTs);
        r.put("item_count", items != null ? items.count : 0);
        r.put("total_price", round2(items != null ? items.totalPrice : 0.0));
        r.put("total_freight", round2(items != null ? items.totalFreight : 0.0));
        r.put("distinct_seller_count", items != null ? items.distinctSellers() : 0);
        r.put("distinct_category_count", items != null ? items.distinctCategories() : 0);
        r.put("product_categories", items != null ? items.categoriesJoined() : null);
        r.put("payment_count", payments != null ? payments.count : 0);
        r.put("payment_types", payments != null ? payments.typesJoined() : null);
        r.put("total_payment_value", round2(payments != null ? payments.totalValue : 0.0));
        r.put("max_installments", payments != null ? payments.maxInstallments() : null);
        r.put("review_count", reviews != null ? reviews.count : 0);
        r.put("review_avg_score", (reviews != null && reviews.count > 0) ? reviews.avg() : null);
        r.put("enriched_at", System.currentTimeMillis());
        return r;
    }

    private static double round2(double v) {
        return Math.round(v * 100.0) / 100.0;
    }
}
