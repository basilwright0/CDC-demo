package com.example.streams;

import com.example.streams.Domain.ItemEnriched;
import com.example.streams.Domain.Payment;
import com.example.streams.Domain.Review;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Per-order aggregates. Each keeps multiset counts so the KTable subtractor can
 * correctly reverse an item/payment/review when the source row changes or is deleted.
 */
final class Aggregates {

    static final class ItemAgg {
        public int count;
        public double totalPrice, totalFreight;
        public Map<String, Integer> sellerCounts = new HashMap<>();
        public Map<String, Integer> categoryCounts = new HashMap<>();

        public ItemAgg() {}

        ItemAgg add(ItemEnriched ie) {
            count++;
            totalPrice += nz(ie.price);
            totalFreight += nz(ie.freight);
            inc(sellerCounts, ie.sellerId);
            inc(categoryCounts, ie.category);
            return this;
        }

        ItemAgg remove(ItemEnriched ie) {
            count--;
            totalPrice -= nz(ie.price);
            totalFreight -= nz(ie.freight);
            dec(sellerCounts, ie.sellerId);
            dec(categoryCounts, ie.category);
            return this;
        }

        int distinctSellers() { return sellerCounts.size(); }
        int distinctCategories() { return categoryCounts.size(); }
        String categoriesJoined() {
            return categoryCounts.isEmpty() ? null
                    : categoryCounts.keySet().stream().sorted().collect(Collectors.joining(", "));
        }
    }

    static final class PaymentAgg {
        public int count;
        public double totalValue;
        public Map<String, Integer> typeCounts = new HashMap<>();
        public Map<Integer, Integer> installmentCounts = new HashMap<>();

        public PaymentAgg() {}

        PaymentAgg add(Payment p) {
            count++;
            totalValue += nz(p.value);
            inc(typeCounts, p.type);
            if (p.installments != null) incInt(installmentCounts, p.installments);
            return this;
        }

        PaymentAgg remove(Payment p) {
            count--;
            totalValue -= nz(p.value);
            dec(typeCounts, p.type);
            if (p.installments != null) decInt(installmentCounts, p.installments);
            return this;
        }

        String typesJoined() {
            return typeCounts.isEmpty() ? null
                    : typeCounts.keySet().stream().sorted().collect(Collectors.joining(", "));
        }

        Integer maxInstallments() {
            return installmentCounts.keySet().stream().max(Integer::compareTo).orElse(null);
        }
    }

    static final class ReviewAgg {
        public int count;
        public long scoreSum;

        public ReviewAgg() {}

        ReviewAgg add(Review r) {
            count++;
            if (r.score != null) scoreSum += r.score;
            return this;
        }

        ReviewAgg remove(Review r) {
            count--;
            if (r.score != null) scoreSum -= r.score;
            return this;
        }

        Double avg() { return count > 0 ? (double) scoreSum / count : null; }
    }

    private static double nz(Double d) { return d == null ? 0.0 : d; }

    private static void inc(Map<String, Integer> m, String k) {
        if (k != null) m.merge(k, 1, Integer::sum);
    }

    private static void dec(Map<String, Integer> m, String k) {
        if (k == null) return;
        Integer v = m.get(k);
        if (v == null) return;
        if (v <= 1) m.remove(k); else m.put(k, v - 1);
    }

    private static void incInt(Map<Integer, Integer> m, Integer k) {
        m.merge(k, 1, Integer::sum);
    }

    private static void decInt(Map<Integer, Integer> m, Integer k) {
        Integer v = m.get(k);
        if (v == null) return;
        if (v <= 1) m.remove(k); else m.put(k, v - 1);
    }

    private Aggregates() {}
}
