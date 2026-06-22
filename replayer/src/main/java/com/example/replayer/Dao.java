package com.example.replayer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.util.List;

/** Thin JDBC layer over the source database with reusable prepared statements. */
final class Dao implements AutoCloseable {

    private final Connection conn;
    private final PreparedStatement insOrder;
    private final PreparedStatement insItem;
    private final PreparedStatement insPayment;
    private final PreparedStatement updApproved;
    private final PreparedStatement updCarrier;
    private final PreparedStatement updDelivered;
    private final PreparedStatement updStatusOnly;
    private final PreparedStatement insReview;

    private Dao(Connection c) throws SQLException {
        this.conn = c;
        insOrder = c.prepareStatement(
                "INSERT INTO orders(order_id, customer_id, order_status, order_purchase_timestamp, "
                        + "order_estimated_delivery_date) VALUES (?,?,?,?,?)");
        insItem = c.prepareStatement(
                "INSERT INTO order_items(order_id, order_item_id, product_id, seller_id, "
                        + "shipping_limit_date, price, freight_value) VALUES (?,?,?,?,?,?,?)");
        insPayment = c.prepareStatement(
                "INSERT INTO order_payments(order_id, payment_sequential, payment_type, "
                        + "payment_installments, payment_value) VALUES (?,?,?,?,?)");
        updApproved = c.prepareStatement(
                "UPDATE orders SET order_status=?, order_approved_at=? WHERE order_id=?");
        updCarrier = c.prepareStatement(
                "UPDATE orders SET order_status=?, order_delivered_carrier_date=? WHERE order_id=?");
        updDelivered = c.prepareStatement(
                "UPDATE orders SET order_status=?, order_delivered_customer_date=? WHERE order_id=?");
        updStatusOnly = c.prepareStatement(
                "UPDATE orders SET order_status=? WHERE order_id=?");
        insReview = c.prepareStatement(
                "INSERT INTO order_reviews(review_id, order_id, review_score, review_comment_title, "
                        + "review_comment_message, review_creation_date, review_answer_timestamp) VALUES (?,?,?,?,?,?,?)");
    }

    static Dao connect(Config cfg) throws SQLException {
        SQLException last = null;
        for (int attempt = 1; attempt <= 30; attempt++) {
            try {
                Connection c = DriverManager.getConnection(cfg.dbUrl, cfg.dbUser, cfg.dbPassword);
                c.setAutoCommit(true);
                System.out.println("Connected to source database.");
                return new Dao(c);
            } catch (SQLException e) {
                last = e;
                System.out.println("DB not ready (attempt " + attempt + "): " + e.getMessage());
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new SQLException("Interrupted while connecting", ie);
                }
            }
        }
        throw last;
    }

    void insertOrderWithChildren(Replayer.OrderRow o, List<Replayer.ItemRow> items,
                                 List<Replayer.PaymentRow> pays) throws SQLException {
        conn.setAutoCommit(false);
        try {
            insOrder.setString(1, o.orderId());
            insOrder.setString(2, o.customerId());
            insOrder.setString(3, "created");
            insOrder.setTimestamp(4, tsOf(o.purchase()));
            insOrder.setTimestamp(5, tsOf(o.estimated()));
            insOrder.executeUpdate();

            for (Replayer.ItemRow it : items) {
                insItem.setString(1, it.orderId());
                insItem.setInt(2, it.itemId());
                insItem.setString(3, it.productId());
                insItem.setString(4, it.sellerId());
                insItem.setTimestamp(5, tsOf(it.shippingLimit()));
                setDouble(insItem, 6, it.price());
                setDouble(insItem, 7, it.freight());
                insItem.addBatch();
            }
            insItem.executeBatch();

            for (Replayer.PaymentRow pay : pays) {
                insPayment.setString(1, pay.orderId());
                insPayment.setInt(2, pay.sequential());
                insPayment.setString(3, pay.type());
                setInt(insPayment, 4, pay.installments());
                setDouble(insPayment, 5, pay.value());
                insPayment.addBatch();
            }
            insPayment.executeBatch();

            conn.commit();
        } catch (SQLException e) {
            conn.rollback();
            throw e;
        } finally {
            conn.setAutoCommit(true);
        }
    }

    void updateStatus(String orderId, String status, String column, LocalDateTime ts) throws SQLException {
        PreparedStatement ps = switch (column) {
            case "order_approved_at" -> updApproved;
            case "order_delivered_carrier_date" -> updCarrier;
            case "order_delivered_customer_date" -> updDelivered;
            default -> throw new IllegalArgumentException("unexpected column: " + column);
        };
        ps.setString(1, status);
        ps.setTimestamp(2, tsOf(ts));
        ps.setString(3, orderId);
        ps.executeUpdate();
    }

    void updateStatusOnly(String orderId, String status) throws SQLException {
        updStatusOnly.setString(1, status);
        updStatusOnly.setString(2, orderId);
        updStatusOnly.executeUpdate();
    }

    void insertReview(Replayer.ReviewRow r) throws SQLException {
        insReview.setString(1, r.reviewId());
        insReview.setString(2, r.orderId());
        setInt(insReview, 3, r.score());
        insReview.setString(4, r.title());
        insReview.setString(5, r.message());
        insReview.setTimestamp(6, tsOf(r.creation()));
        insReview.setTimestamp(7, tsOf(r.answer()));
        insReview.executeUpdate();
    }

    /** Delete in FK-safe order (children before parent). */
    void clearLifecycleTables() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.executeUpdate("DELETE FROM order_reviews");
            s.executeUpdate("DELETE FROM order_items");
            s.executeUpdate("DELETE FROM order_payments");
            s.executeUpdate("DELETE FROM orders");
        }
    }

    private static Timestamp tsOf(LocalDateTime t) {
        return t == null ? null : Timestamp.valueOf(t);
    }

    private static void setDouble(PreparedStatement ps, int idx, Double v) throws SQLException {
        if (v == null) ps.setNull(idx, Types.DECIMAL);
        else ps.setDouble(idx, v);
    }

    private static void setInt(PreparedStatement ps, int idx, Integer v) throws SQLException {
        if (v == null) ps.setNull(idx, Types.INTEGER);
        else ps.setInt(idx, v);
    }

    @Override
    public void close() throws SQLException {
        conn.close();
    }
}
