# connect/ — Kafka Connect image & connector configs

`Dockerfile` builds the Connect worker used by `docker-compose.yml` (service `connect`).
It extends `confluentinc/cp-kafka-connect:8.2.2` and adds the Debezium MySQL connector
(`DEBEZIUM_VERSION`, default `3.5.2.Final`). The base image already ships the Confluent
Avro converter; note it lacks `tar`/`gzip`, so the Dockerfile installs them via `microdnf`
before extracting the plugin.

## Connectors

- `debezium-source.json` (step 3) — Debezium MySQL **source**. Captures the 8 join tables,
  unwraps the envelope with `ExtractNewRecordState`, emits Avro to Schema Registry. Register:
  ```bash
  curl -X POST -H "Content-Type: application/json" \
    --data @connect/debezium-source.json http://localhost:8083/connectors
  ```
- `jdbc-sink.json` (step 6) — Debezium JDBC **sink** into `mysql-sink`. `insert.mode=upsert`,
  `primary.key.mode=record_value`, `primary.key.fields=order_id`, `schema.evolution=basic`
  (auto-creates the table). Key converter is `StringConverter` (the `order_enriched` key is a
  plain string); value converter is Avro. Register:
  ```bash
  curl -X POST -H "Content-Type: application/json" \
    --data @connect/jdbc-sink.json http://localhost:8083/connectors
  ```
  The Dockerfile installs this connector plus the MySQL JDBC driver (not bundled, for
  licensing reasons).

## Handy

```bash
curl -s http://localhost:8083/connector-plugins | jq '.[].class'      # installed plugins
curl -s http://localhost:8083/connectors                              # registered connectors
curl -s http://localhost:8083/connectors/olist-mysql-source/status    # state
curl -X DELETE http://localhost:8083/connectors/olist-mysql-source    # remove
```
