-- Least-privilege user for the Debezium MySQL connector (step 3).
-- mysql_native_password keeps the binlog-client handshake simple.
CREATE USER IF NOT EXISTS 'debezium'@'%' IDENTIFIED WITH mysql_native_password BY 'dbz';

-- Privileges required by the Debezium MySQL connector:
--   SELECT            read table rows during snapshot
--   RELOAD            FLUSH for a consistent snapshot
--   SHOW DATABASES    enumerate databases
--   REPLICATION SLAVE read the binlog stream
--   REPLICATION CLIENT read binlog position / status
--   LOCK TABLES       brief lock during the initial snapshot
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT, LOCK TABLES
  ON *.* TO 'debezium'@'%';

FLUSH PRIVILEGES;
