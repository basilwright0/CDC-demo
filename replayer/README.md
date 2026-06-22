# replayer/ — Olist lifecycle replayer (step 4)

Java 21 service (Gradle, `application` plugin) that turns the static Olist CSVs into a
realistic stream of database changes against `mysql-source`, so Debezium produces authentic
CDC (inserts **and** updates), not a single bulk snapshot.

## How it works

The dimension tables (customers, products, sellers, geolocation, translation) are already
loaded by the source DB init. This service replays only the **order lifecycle**:

1. `order_purchase_timestamp` → INSERT order (status `created`) + its items + payment
2. `order_approved_at` → UPDATE status `approved`
3. `order_delivered_carrier_date` → UPDATE status `shipped`
4. `order_delivered_customer_date` → UPDATE status `delivered`
5. terminal correction → UPDATE to the dataset's final status if different (e.g. `canceled`)
6. review creation time → INSERT into order_reviews

All events across all orders are merged and sorted by `(timestamp, phase)`, then replayed
with time compression. Sub-event timestamps are clamped to ≥ the order's purchase time so
the INSERT always precedes its updates/reviews (FK-safe).

## Run

```bash
docker compose build replayer
docker compose run --rm replayer
```

Env knobs (see defaults in `docker-compose.yml`):

| Var | Default | Meaning |
|-----|---------|---------|
| `REPLAY_SPEEDUP` | 120000 | real milliseconds ÷ this = sleep between events (higher = faster) |
| `REPLAY_MAX_SLEEP_MS` | 200 | cap on the sleep between consecutive events |
| `REPLAY_LIMIT_ORDERS` | 0 | replay only the first N orders (0 = all); handy for quick tests |
| `REPLAY_TRUNCATE_FIRST` | true | clear the four lifecycle tables before replaying (re-runnable) |
| `DB_URL` / `DB_USER` / `DB_PASSWORD` | app/app @ mysql-source | JDBC connection |

Connects as the least-privilege `app` user (created by `mysql/source/initdb.d/04_app_user.sql`).

## Source layout

- `src/main/java/com/example/replayer/Replayer.java` — load, build event timeline, pace
- `src/main/java/com/example/replayer/Dao.java` — JDBC with reusable prepared statements
- `src/main/java/com/example/replayer/Config.java` — env-driven config
