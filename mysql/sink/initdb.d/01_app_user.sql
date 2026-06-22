-- Writer user for the JDBC sink connector. ALL on the read-model DB so the sink
-- can auto-create the order_enriched table and upsert into it.
CREATE USER IF NOT EXISTS 'app'@'%' IDENTIFIED WITH mysql_native_password BY 'app';
GRANT ALL PRIVILEGES ON olist_readmodel.* TO 'app'@'%';
FLUSH PRIVILEGES;
