# streams-app/ — Kafka Streams denormalizer (step 5)

Java 21 + Gradle Kafka Streams application. Consumes the eight Debezium CDC topics and
produces a flat `order_enriched` topic keyed by `order_id` — the continuously-updated read
model.

## Topology

```
products ⨝ translation (FK on category)            → product+English category
order_items ⨝ that (FK on product_id)              → items with category
        └─ groupBy order_id → item aggregate (count, totals, distinct sellers/categories)
order_payments  → groupBy order_id → payment aggregate (count, total, types, max installments)
order_reviews   → groupBy order_id → review aggregate (count, avg score)

orders ⨝ customers (FK on customer_id)
       ⨝ item aggregate   (equi-join on order_id)
       ⨝ payment aggregate
       ⨝ review aggregate                          → order_enriched (Avro)
```

- Input/output values: Avro via Confluent Schema Registry (`GenericAvroSerde`).
- Internal state (POJOs, aggregates): JSON (Jackson) — see `JsonSerde`.
- `processing.guarantee=exactly_once_v2`; with idempotent CDC upserts upstream this gives
  effectively-once row state end to end.
- Output schema: `src/main/resources/order_enriched.avsc` (flat, so it sinks to one MySQL row).

## Run

```bash
docker compose up -d --build streams-app
```

Needs the CDC topics to already exist (register the Debezium connector first), hence the
`app` profile. Env: `BOOTSTRAP_SERVERS`, `SCHEMA_REGISTRY_URL`, `APPLICATION_ID`.

## Tests

- **Unit** — `DenormalizationTopologyTest` drives the full topology with `TopologyTestDriver`
  against a mock Schema Registry (no broker needed) and asserts the enriched output for a
  sample order. Runs during the Docker image build (`gradle test installDist`):
  ```bash
  docker compose build streams-app   # compiles + runs the unit test
  ```
- **Integration** — `src/integrationTest/.../PipelineIntegrationTest` runs the real topology
  against an ephemeral Kafka broker (Testcontainers) with a mock Schema Registry, producing
  Debezium-shaped Avro and asserting the `order_enriched` output over the wire. Separate source
  set so it's excluded from the image build (which has no Docker daemon). Run from a
  Docker-enabled host:
  ```bash
  gradle integrationTest
  ```

## Layout

- `DenormalizationTopology.java` — the topology
- `Domain.java` / `Aggregates.java` / `Enriched.java` — value objects, aggregates, output row
- `Avro.java` / `JsonSerde.java` — serde helpers
- `StreamsApp.java` — config + lifecycle
