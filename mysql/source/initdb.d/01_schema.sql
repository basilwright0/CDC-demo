-- Olist source schema (normalized OLTP).
-- Dimension/reference tables are bulk-loaded at init (02_load_dimensions.sql).
-- The order lifecycle tables are created empty and filled over time by the
-- replayer (step 4) so Debezium emits a realistic stream of inserts/updates.

SET NAMES utf8mb4;

-- ---------- dimension / reference tables ----------------------------------

CREATE TABLE customers (
  customer_id              CHAR(32)    NOT NULL,
  customer_unique_id       CHAR(32)    NOT NULL,
  customer_zip_code_prefix VARCHAR(8)  NULL,
  customer_city            VARCHAR(64) NULL,
  customer_state           CHAR(2)     NULL,
  PRIMARY KEY (customer_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE sellers (
  seller_id              CHAR(32)    NOT NULL,
  seller_zip_code_prefix VARCHAR(8)  NULL,
  seller_city            VARCHAR(64) NULL,
  seller_state           CHAR(2)     NULL,
  PRIMARY KEY (seller_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE products (
  product_id                 CHAR(32)    NOT NULL,
  product_category_name      VARCHAR(64) NULL,
  product_name_length        INT         NULL,  -- 'lenght' (sic) in the CSV header
  product_description_length INT         NULL,
  product_photos_qty         INT         NULL,
  product_weight_g           INT         NULL,
  product_length_cm          INT         NULL,
  product_height_cm          INT         NULL,
  product_width_cm           INT         NULL,
  PRIMARY KEY (product_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE product_category_name_translation (
  product_category_name         VARCHAR(64) NOT NULL,
  product_category_name_english VARCHAR(64) NULL,
  PRIMARY KEY (product_category_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- No natural PK (zip prefix repeats); surrogate key. Not captured by Debezium.
CREATE TABLE geolocation (
  id                          BIGINT      NOT NULL AUTO_INCREMENT,
  geolocation_zip_code_prefix VARCHAR(8)  NULL,
  geolocation_lat             DOUBLE      NULL,
  geolocation_lng             DOUBLE      NULL,
  geolocation_city            VARCHAR(64) NULL,
  geolocation_state           CHAR(2)     NULL,
  PRIMARY KEY (id),
  KEY ix_geo_zip (geolocation_zip_code_prefix)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------- order lifecycle tables (populated by the replayer) -------------

CREATE TABLE orders (
  order_id                      CHAR(32)    NOT NULL,
  customer_id                   CHAR(32)    NOT NULL,
  order_status                  VARCHAR(20) NULL,
  order_purchase_timestamp      DATETIME    NULL,
  order_approved_at             DATETIME    NULL,
  order_delivered_carrier_date  DATETIME    NULL,
  order_delivered_customer_date DATETIME    NULL,
  order_estimated_delivery_date DATETIME    NULL,
  PRIMARY KEY (order_id),
  KEY ix_orders_customer (customer_id),
  CONSTRAINT fk_orders_customer FOREIGN KEY (customer_id) REFERENCES customers (customer_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE order_items (
  order_id            CHAR(32)      NOT NULL,
  order_item_id       INT           NOT NULL,
  product_id          CHAR(32)      NOT NULL,
  seller_id           CHAR(32)      NOT NULL,
  shipping_limit_date DATETIME      NULL,
  price               DECIMAL(10,2) NULL,
  freight_value       DECIMAL(10,2) NULL,
  PRIMARY KEY (order_id, order_item_id),
  KEY ix_items_product (product_id),
  KEY ix_items_seller (seller_id),
  CONSTRAINT fk_items_order FOREIGN KEY (order_id) REFERENCES orders (order_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE order_payments (
  order_id             CHAR(32)      NOT NULL,
  payment_sequential   INT           NOT NULL,
  payment_type         VARCHAR(20)   NULL,
  payment_installments INT           NULL,
  payment_value        DECIMAL(10,2) NULL,
  PRIMARY KEY (order_id, payment_sequential),
  CONSTRAINT fk_payments_order FOREIGN KEY (order_id) REFERENCES orders (order_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- review_id is not unique in this dataset, so use a surrogate PK and index the
-- natural keys. The Streams app re-keys reviews by order_id for the join.
CREATE TABLE order_reviews (
  review_pk               BIGINT       NOT NULL AUTO_INCREMENT,
  review_id               CHAR(32)     NOT NULL,
  order_id                CHAR(32)     NOT NULL,
  review_score            TINYINT      NULL,
  review_comment_title    VARCHAR(255) NULL,
  review_comment_message  TEXT         NULL,
  review_creation_date    DATETIME     NULL,
  review_answer_timestamp DATETIME     NULL,
  PRIMARY KEY (review_pk),
  KEY ix_reviews_order (order_id),
  KEY ix_reviews_review (review_id),
  CONSTRAINT fk_reviews_order FOREIGN KEY (order_id) REFERENCES orders (order_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
