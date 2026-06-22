# observability/ — Prometheus + Grafana (step 7)

Metrics for the pipeline, behind the `observability` compose profile:

```bash
docker compose --profile observability up -d
```

- **kafka-exporter** (`danielqsj/kafka-exporter`) connects to Kafka as a client and exposes
  consumer-group lag and per-topic/partition offsets on `:9308` — no changes to the broker.
- **prometheus** (`prometheus.yml`) scrapes kafka-exporter every 10s — http://localhost:9090
- **grafana** auto-provisions the Prometheus datasource and a dashboard — http://localhost:3000
  (anonymous viewer; `admin`/`admin` to edit)

## Dashboard

`grafana/dashboards/cdc_pipeline.json` — "CDC denormalization pipeline":
- Consumer-group lag (per group) — watch `olist-denormalizer` (Streams app) and
  `connect-olist-readmodel-sink` (JDBC sink) stay near zero
- Topic throughput (records/s) for the source orders and `order_enriched`
- Total records per topic

## Files

```
prometheus.yml
grafana/provisioning/datasources/prometheus.yml   # datasource (uid: prometheus)
grafana/provisioning/dashboards/provider.yml       # dashboard file provider
grafana/dashboards/cdc_pipeline.json               # the dashboard
```
