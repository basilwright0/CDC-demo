# Near-real-time CDC denormalization pipeline

A normalized MySQL OLTP database is captured with **Debezium** change data capture, streamed
through **Kafka**, denormalized in near-real-time by a **Kafka Streams** application
(foreign-key joins + aggregations), and written back into a separate MySQL **read model** by a
**Kafka Connect JDBC sink**. Everything runs in Docker.

It's the streaming equivalent of a continuously-maintained materialized view — a CQRS-style
read model kept in sync with its source of truth, updated as each order moves through its
lifecycle.

## Architecture

```
  Olist CSVs
      │   replayer — replays each order's lifecycle as a timestamped INSERT/UPDATE stream
      ▼
  MySQL · source  (normalized OLTP)
      │   binlog
      ▼
  Debezium source connector  ──▶  per-table CDC topics  ┐
  (Kafka Connect)                                        │   Avro · Schema Registry
                                                         ▼
                          Kafka Streams app — KTable FK joins + per-order aggregations
                                                         │
                                                         ▼
                                          order_enriched  (one flat row per order)
                                                         │
                                                         ▼
                          Debezium JDBC sink connector — upsert by order_id
                                                         │
                                                         ▼
                                  MySQL · sink  (read model — separate instance)

  Observe:  Kafka UI   ·   Prometheus + Grafana  (consumer lag, throughput)
```

The source and sink are **separate MySQL instances** by design: if the sink wrote back into
the database Debezium watches, those writes would re-enter the binlog and create a CDC
feedback loop.

## Tech stack

| Concern | Choice |
|---|---|
| Source / sink DB | MySQL 8.4 LTS |
| Change data capture | Debezium 3.5 (Kafka Connect source) |
| Broker | Apache Kafka 4.2 in **KRaft** mode (Confluent Platform 8.2.2, no ZooKeeper) |
| Stream processing | Kafka Streams · **Java 21** · Gradle |
| Serialization | **Avro** + Confluent Schema Registry |
| Read-model sink | Debezium JDBC sink (upsert) |
| Observability | Kafka UI (kafbat) · Prometheus + Grafana |
| Delivery guarantee | Debezium at-least-once + Streams `exactly_once_v2` + idempotent upsert ≈ effectively-once |

## What this demonstrates

- **CDC** with Debezium — binlog capture, initial snapshots, the `ExtractNewRecordState` SMT
- **Kafka ecosystem** — KRaft, Schema Registry + Avro, Kafka Connect source **and** sink, custom Connect image
- **Stateful stream processing** — Kafka Streams `KTable`–`KTable` foreign-key joins (KIP-213),
  multiset aggregations with correct add/subtract, exactly-once
- **Data modeling** — normalized OLTP → denormalized read model (CQRS / materialized view)
- **Reproducible infra** — Docker Compose with healthchecks, dependency ordering, and profiles
- **Testing** — unit (`TopologyTestDriver`) and integration (Testcontainers); plus a live e2e check

## Quickstart

Prerequisites: Docker Desktop (Compose v2). Download the nine
[Olist CSVs](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) (keep their original
names) into **`data/raw/`** — they must be present before the first `docker compose up`,
because the source DB loads them on initialization.

```bash
# 1 · start the infrastructure (Kafka, Schema Registry, Connect, 2× MySQL, Kafka UI)
docker compose up -d

# 2 · register the Debezium source connector (snapshots the dimensions, starts CDC)
curl -X POST -H "Content-Type: application/json" --data @connect/debezium-source.json http://localhost:8083/connectors

# 3 · replay the order lifecycle into the source DB (a few minutes; add -e REPLAY_LIMIT_ORDERS=2000 for a quick partial run)
docker compose run --rm replayer

# 4 · start the Kafka Streams denormalizer
docker compose up -d --build streams-app

# 5 · register the JDBC sink connector (writes the read model back to MySQL)
curl -X POST -H "Content-Type: application/json" --data @connect/jdbc-sink.json http://localhost:8083/connectors

# 6 · (optional) start metrics
docker compose --profile observability up -d
```

Verify the loop — the denormalized read model should fill up:

```bash
docker exec mysql-sink mysql -uroot -prootpw olist_readmodel -e \
  "SELECT order_id, customer_city, item_count, total_price, product_categories, review_avg_score FROM order_enriched LIMIT 5;"
```

Tear down with `docker compose --profile app --profile observability down` (add `-v` to wipe
the data volumes).

> The order matters: the Streams app reads the lifecycle topics, which only exist once the
> replayer has inserted rows — so register the source and run the replayer before starting it.

## How it works

### Source database

On first init, `mysql-source` runs [mysql/source/initdb.d/](mysql/source/initdb.d/): the
normalized Olist schema (9 tables, FKs on the order graph), a bulk `LOAD DATA INFILE` of the
reference tables (`customers`, `sellers`, `products`, category translation, `geolocation`),
and a least-privilege `debezium` user. The four **order-lifecycle** tables are left empty —
the replayer fills them so Debezium emits a realistic change stream rather than one big
snapshot. Binlog settings (`ROW`, `binlog_row_image=FULL`, GTID) are passed as `mysqld` flags
in `docker-compose.yml`.

### Change data capture

`connect` is built from [connect/Dockerfile](connect/Dockerfile) (Confluent Connect + the
Debezium MySQL source + the JDBC sink + the MySQL driver). The source connector
([connect/debezium-source.json](connect/debezium-source.json)) captures the 8 join tables
(geolocation excluded), unwraps Debezium's envelope with `ExtractNewRecordState` so each topic
carries flat row state (materializable as a `KTable`; deletes become tombstones), and emits
Avro to Schema Registry. Money uses `decimal.handling.mode=double` to keep downstream code
simple.

### Order-lifecycle replayer

[replayer/](replayer/README.md) (Java 21) reads the order CSVs and replays each order against
`mysql-source` in **global timestamp order**: INSERT at purchase (with its items + payment),
an UPDATE per lifecycle timestamp (approved → shipped → delivered), a final status correction
for terminal states (e.g. canceled), and a review INSERT at its creation time. Time is
compressed so two years of history replay in minutes. Because each order goes through several
updates, the `orders` topic carries far more records than there are orders — that's the change
stream. Tunables (env): `REPLAY_SPEEDUP`, `REPLAY_MAX_SLEEP_MS`, `REPLAY_LIMIT_ORDERS`,
`REPLAY_TRUNCATE_FIRST`.

### Streaming denormalizer

[streams-app/](streams-app/README.md) (Java 21 Kafka Streams) materializes the eight CDC
topics as `KTable`s and produces a flat `order_enriched` topic keyed by `order_id`:

- orders ⨝ customers (KTable **foreign-key** join)
- per-order item aggregates (count, totals, distinct sellers), where each item's product
  category is resolved via products ⨝ category-translation (a second FK join)
- per-order payment aggregates (count, total, types, max installments)
- per-order review aggregate (count, average score)

Input/output values are Avro; internal aggregate state is JSON. Runs with
`processing.guarantee=exactly_once_v2`. The output is the read model's change log — each order
re-emits as its items, payments, and reviews arrive, so the latest record per `order_id` is
the current view.

### Read-model sink

The Debezium JDBC sink ([connect/jdbc-sink.json](connect/jdbc-sink.json)) consumes
`order_enriched` and **upserts** it into `mysql-sink`, closing the loop. Key settings:
`insert.mode=upsert`, `primary.key.mode=record_value`, `primary.key.fields=order_id`,
`schema.evolution=basic` (auto-creates the table), `key.converter=StringConverter` (the topic
key is a plain string) + `value.converter=AvroConverter`.

## Observability

Optional Prometheus + Grafana ([observability/](observability/README.md)), behind the
`observability` profile. `kafka-exporter` exposes consumer-group lag and topic offsets;
Grafana auto-provisions a "CDC denormalization pipeline" dashboard showing lag for the two
pipeline consumer groups (`olist-denormalizer`, `connect-olist-readmodel-sink`) and topic
throughput. Grafana: http://localhost:3000 (anonymous viewer; `admin`/`admin` to edit).

## Testing

- **Unit** — `DenormalizationTopologyTest` drives the topology with `TopologyTestDriver` and a
  mock Schema Registry (no broker). Runs in the Streams image build (`gradle test`).
- **Integration** — `PipelineIntegrationTest` (`streams-app/src/integrationTest`) runs the real
  topology against an ephemeral Kafka broker via **Testcontainers**, producing Debezium-shaped
  Avro and asserting the enriched output over the wire. Run from a Docker-enabled host with
  `gradle integrationTest` (Docker Desktop's in-container socket is a proxy Testcontainers
  can't drive, so it runs on the host or Linux CI, not docker-in-docker).
- **Live end-to-end** — with the stack running, insert an order and watch it reach the sink:
  ```bash
  docker exec mysql-source mysql -uroot -prootpw olist -e "INSERT INTO orders(order_id,customer_id,order_status,order_purchase_timestamp) SELECT 'smoketest0000000000000000000001', customer_id, 'delivered', NOW() FROM customers LIMIT 1;"
  docker exec mysql-sink mysql -uroot -prootpw olist_readmodel -e "SELECT order_id, customer_city, order_status FROM order_enriched WHERE order_id='smoketest0000000000000000000001';"
  ```

## Endpoints & credentials

| Service | URL / port | Notes |
|---|---|---|
| Kafka UI | http://localhost:8088 | topics, schemas, connectors, lag |
| Grafana | http://localhost:3000 | metrics dashboard (`observability` profile) |
| Prometheus | http://localhost:9090 | (`observability` profile) |
| Schema Registry | http://localhost:8081 | `GET /subjects` |
| Kafka Connect | http://localhost:8083 | `GET /connectors` |
| Kafka (host clients) | `localhost:29092` | bootstrap for host-run apps/tests |
| MySQL source | `localhost:3306` | db `olist` |
| MySQL sink | `localhost:3307` | db `olist_readmodel` |

All credentials in this repo (e.g. MySQL `root` / `rootpw`) are **local-demo-only**.

## Project layout

```
docker-compose.yml          the whole stack (core + replay/app/observability profiles)
data/raw/                   Olist CSVs (git-ignored)
mysql/source/initdb.d/      schema, dimension load, debezium + app users
mysql/sink/initdb.d/        read-model writer user
connect/                    Connect image + connector configs (source, sink)
replayer/                   Java — replays the order lifecycle into the source DB
streams-app/                Java — Kafka Streams denormalizer (+ unit & integration tests)
observability/              Prometheus + Grafana
```

## Notes & gotchas

A few real issues caught by running things rather than assuming:

- **MySQL ignores world-writable config files**, and Windows bind mounts arrive
  world-writable — so a mounted `my.cnf` is silently dropped. Binlog settings are passed as
  `mysqld` flags instead.
- The **Confluent Connect 8.2.2 image ships `curl` but not `tar`/`gzip`**, so the Dockerfile
  installs them via `microdnf` before extracting connector plugins.
- Debezium 3.5 **renamed several connector properties** (the unwrap SMT's tombstone option, the
  JDBC sink's `primary.key.mode` / `collection.name.format`); these were verified against the
  3.5.2 source before registering connectors.
- One Olist CSV is **CRLF** while the others are LF, leaving a trailing `\r` on a category
  column. Fixed in the loader, and the live fix propagated through the whole topology
  (translation → products → items → aggregate → read model) on its own.

## Dataset & license

[Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
— ~100k real (anonymized) orders across ~8 normalized tables, **CC BY-NC-SA 4.0**
(non-commercial; fine for a portfolio).
