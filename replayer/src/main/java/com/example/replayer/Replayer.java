package com.example.replayer;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Replays the Olist order lifecycle against the source MySQL database in global
 * timestamp order, so Debezium produces a realistic stream of inserts and updates.
 *
 * Each order yields: an INSERT at purchase time (with its items + payment), then an
 * UPDATE per lifecycle timestamp (approved / shipped / delivered), then a final status
 * correction if the dataset's terminal status differs (e.g. canceled). Reviews are
 * inserted at their creation time. All events are merged, sorted by (timestamp, phase),
 * and replayed with time compression.
 */
public class Replayer {

    static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    record OrderRow(String orderId, String customerId, String status,
                    LocalDateTime purchase, LocalDateTime approved, LocalDateTime carrier,
                    LocalDateTime delivered, LocalDateTime estimated) {}
    record ItemRow(String orderId, int itemId, String productId, String sellerId,
                   LocalDateTime shippingLimit, Double price, Double freight) {}
    record PaymentRow(String orderId, int sequential, String type, Integer installments, Double value) {}
    record ReviewRow(String reviewId, String orderId, Integer score, String title, String message,
                     LocalDateTime creation, LocalDateTime answer) {}

    @FunctionalInterface interface SqlAction { void run(Dao dao) throws SQLException; }
    record Event(long ts, int phase, SqlAction action) {}

    public static void main(String[] args) throws Exception {
        Config cfg = Config.fromEnv();
        System.out.printf("Replayer starting: dataDir=%s speedup=%d maxSleepMs=%d limitOrders=%d truncateFirst=%s%n",
                cfg.dataDir, cfg.speedup, cfg.maxSleepMs, cfg.limitOrders, cfg.truncateFirst);

        Path dir = Path.of(cfg.dataDir);

        List<OrderRow> orders = loadOrders(dir.resolve("olist_orders_dataset.csv"));
        orders.removeIf(o -> o.purchase() == null); // can't place on the timeline
        if (cfg.limitOrders > 0 && orders.size() > cfg.limitOrders) {
            orders = new ArrayList<>(orders.subList(0, cfg.limitOrders));
        }
        Set<String> orderIds = new HashSet<>();
        Map<String, LocalDateTime> purchaseByOrder = new HashMap<>();
        for (OrderRow o : orders) {
            orderIds.add(o.orderId());
            purchaseByOrder.put(o.orderId(), o.purchase());
        }

        Map<String, List<ItemRow>> items = loadItems(dir.resolve("olist_order_items_dataset.csv"), orderIds);
        Map<String, List<PaymentRow>> payments = loadPayments(dir.resolve("olist_order_payments_dataset.csv"), orderIds);
        List<ReviewRow> reviews = loadReviews(dir.resolve("olist_order_reviews_dataset.csv"), orderIds);

        System.out.printf("Loaded: orders=%d itemGroups=%d paymentGroups=%d reviews=%d%n",
                orders.size(), items.size(), payments.size(), reviews.size());

        List<Event> events = new ArrayList<>(orders.size() * 3 + reviews.size());
        for (OrderRow o : orders) {
            long pts = epoch(o.purchase());
            List<ItemRow> its = items.getOrDefault(o.orderId(), List.of());
            List<PaymentRow> pays = payments.getOrDefault(o.orderId(), List.of());
            events.add(new Event(pts, 0, dao -> dao.insertOrderWithChildren(o, its, pays)));

            String last = "created";
            if (o.approved() != null) {
                events.add(new Event(Math.max(epoch(o.approved()), pts), 1,
                        dao -> dao.updateStatus(o.orderId(), "approved", "order_approved_at", o.approved())));
                last = "approved";
            }
            if (o.carrier() != null) {
                events.add(new Event(Math.max(epoch(o.carrier()), pts), 2,
                        dao -> dao.updateStatus(o.orderId(), "shipped", "order_delivered_carrier_date", o.carrier())));
                last = "shipped";
            }
            if (o.delivered() != null) {
                events.add(new Event(Math.max(epoch(o.delivered()), pts), 3,
                        dao -> dao.updateStatus(o.orderId(), "delivered", "order_delivered_customer_date", o.delivered())));
                last = "delivered";
            }
            if (o.status() != null && !o.status().equals(last)) {
                long ft = maxTs(pts, o.approved(), o.carrier(), o.delivered());
                String finalStatus = o.status();
                events.add(new Event(ft, 4, dao -> dao.updateStatusOnly(o.orderId(), finalStatus)));
            }
        }
        for (ReviewRow rv : reviews) {
            LocalDateTime base = rv.creation() != null ? rv.creation() : rv.answer();
            if (base == null) continue;
            LocalDateTime p = purchaseByOrder.get(rv.orderId());
            long rts = (p != null) ? Math.max(epoch(base), epoch(p)) : epoch(base);
            events.add(new Event(rts, 5, dao -> dao.insertReview(rv)));
        }

        events.sort(Comparator.comparingLong(Event::ts).thenComparingInt(Event::phase));
        System.out.printf("Built %d events; replaying...%n", events.size());

        try (Dao dao = Dao.connect(cfg)) {
            if (cfg.truncateFirst) {
                System.out.println("Clearing lifecycle tables (orders/items/payments/reviews)...");
                dao.clearLifecycleTables();
            }
            long startWall = System.currentTimeMillis();
            long prevTs = events.isEmpty() ? 0 : events.get(0).ts();
            int n = 0;
            for (Event e : events) {
                long gap = e.ts() - prevTs;
                if (gap > 0) {
                    long sleep = Math.min(gap / cfg.speedup, cfg.maxSleepMs);
                    if (sleep > 0) Thread.sleep(sleep);
                }
                prevTs = e.ts();
                e.action().run(dao);
                if (++n % 20000 == 0) {
                    System.out.printf("  %d/%d events (sim time %s, %.1fs wall)%n",
                            n, events.size(),
                            LocalDateTime.ofEpochSecond(e.ts() / 1000, 0, ZoneOffset.UTC).format(TS),
                            (System.currentTimeMillis() - startWall) / 1000.0);
                }
            }
            System.out.printf("Done: replayed %d events in %.1fs.%n", n, (System.currentTimeMillis() - startWall) / 1000.0);
        }
    }

    static long epoch(LocalDateTime t) {
        return t.toInstant(ZoneOffset.UTC).toEpochMilli();
    }

    static long maxTs(long base, LocalDateTime... candidates) {
        long m = base;
        for (LocalDateTime t : candidates) if (t != null) m = Math.max(m, epoch(t));
        return m;
    }

    // ---------- CSV loaders ----------

    static List<OrderRow> loadOrders(Path p) throws IOException {
        List<OrderRow> out = new ArrayList<>();
        try (CSVParser parser = open(p)) {
            for (CSVRecord r : parser) {
                out.add(new OrderRow(
                        r.get("order_id"), r.get("customer_id"), blankToNull(r.get("order_status")),
                        ts(r.get("order_purchase_timestamp")), ts(r.get("order_approved_at")),
                        ts(r.get("order_delivered_carrier_date")), ts(r.get("order_delivered_customer_date")),
                        ts(r.get("order_estimated_delivery_date"))));
            }
        }
        return out;
    }

    static Map<String, List<ItemRow>> loadItems(Path p, Set<String> keep) throws IOException {
        Map<String, List<ItemRow>> out = new HashMap<>();
        try (CSVParser parser = open(p)) {
            for (CSVRecord r : parser) {
                String oid = r.get("order_id");
                if (!keep.contains(oid)) continue;
                out.computeIfAbsent(oid, k -> new ArrayList<>()).add(new ItemRow(
                        oid, parseInt(r.get("order_item_id")), r.get("product_id"), r.get("seller_id"),
                        ts(r.get("shipping_limit_date")), parseDouble(r.get("price")), parseDouble(r.get("freight_value"))));
            }
        }
        return out;
    }

    static Map<String, List<PaymentRow>> loadPayments(Path p, Set<String> keep) throws IOException {
        Map<String, List<PaymentRow>> out = new HashMap<>();
        try (CSVParser parser = open(p)) {
            for (CSVRecord r : parser) {
                String oid = r.get("order_id");
                if (!keep.contains(oid)) continue;
                out.computeIfAbsent(oid, k -> new ArrayList<>()).add(new PaymentRow(
                        oid, parseInt(r.get("payment_sequential")), r.get("payment_type"),
                        parseInt(r.get("payment_installments")), parseDouble(r.get("payment_value"))));
            }
        }
        return out;
    }

    static List<ReviewRow> loadReviews(Path p, Set<String> keep) throws IOException {
        List<ReviewRow> out = new ArrayList<>();
        long skipped = 0;
        try (CSVParser parser = open(p)) {
            for (CSVRecord r : parser) {
                try {
                    String oid = r.get("order_id");
                    if (!keep.contains(oid)) continue;
                    out.add(new ReviewRow(
                            r.get("review_id"), oid, parseInt(r.get("review_score")),
                            blankToNull(r.get("review_comment_title")), blankToNull(r.get("review_comment_message")),
                            ts(r.get("review_creation_date")), ts(r.get("review_answer_timestamp"))));
                } catch (RuntimeException ex) {
                    skipped++;
                }
            }
        }
        if (skipped > 0) System.out.printf("  (skipped %d malformed review rows)%n", skipped);
        return out;
    }

    static CSVParser open(Path p) throws IOException {
        Reader reader = Files.newBufferedReader(p);
        return CSVFormat.DEFAULT.builder()
                .setHeader()
                .setSkipHeaderRecord(true)
                .build()
                .parse(reader);
    }

    static LocalDateTime ts(String s) {
        s = blankToNull(s);
        return s == null ? null : LocalDateTime.parse(s, TS);
    }

    static String blankToNull(String s) {
        return (s == null || s.isBlank()) ? null : s;
    }

    static Integer parseInt(String s) {
        s = blankToNull(s);
        return s == null ? null : Integer.valueOf(s);
    }

    static Double parseDouble(String s) {
        s = blankToNull(s);
        return s == null ? null : Double.valueOf(s);
    }
}
