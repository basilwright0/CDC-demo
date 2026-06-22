-- Application user used by the replayer to write the order lifecycle tables.
-- mysql_native_password keeps the JDBC handshake simple.
CREATE USER IF NOT EXISTS 'app'@'%' IDENTIFIED WITH mysql_native_password BY 'app';
GRANT SELECT, INSERT, UPDATE, DELETE ON olist.* TO 'app'@'%';
FLUSH PRIVILEGES;
