package com.example.streams;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;

import java.util.Map;

/** Helpers for reading Debezium-unwrapped Avro records and building Avro serdes. */
final class Avro {

    static String s(GenericRecord r, String field) {
        Object o = r.get(field);
        return o == null ? null : o.toString();
    }

    static Long l(GenericRecord r, String field) {
        Object o = r.get(field);
        return o == null ? null : ((Number) o).longValue();
    }

    static Integer i(GenericRecord r, String field) {
        Object o = r.get(field);
        return o == null ? null : ((Number) o).intValue();
    }

    static Double d(GenericRecord r, String field) {
        Object o = r.get(field);
        return o == null ? null : ((Number) o).doubleValue();
    }

    static Serde<GenericRecord> serde(Map<String, String> config, boolean isKey) {
        GenericAvroSerde serde = new GenericAvroSerde();
        serde.configure(config, isKey);
        return serde;
    }

    private Avro() {}
}
