# Kafka Connect worker with the Debezium MySQL source connector installed.
# The Confluent Avro converter is already present in the base image, so we only
# add Debezium. Pinned to a specific Debezium release for reproducibility.
FROM confluentinc/cp-kafka-connect:8.2.2

ARG DEBEZIUM_VERSION=3.5.2.Final
ARG MYSQL_DRIVER_VERSION=8.4.0
ARG MAVEN=https://repo1.maven.org/maven2

USER root
# curl is present in the base image but tar/gzip were stripped, so add them first.
RUN microdnf install -y tar gzip && microdnf clean all && \
    mkdir -p /usr/share/confluent-hub-components && \
    # Debezium MySQL source connector
    curl -fsSL "${MAVEN}/io/debezium/debezium-connector-mysql/${DEBEZIUM_VERSION}/debezium-connector-mysql-${DEBEZIUM_VERSION}-plugin.tar.gz" -o /tmp/dbz-mysql.tar.gz && \
    tar -xzf /tmp/dbz-mysql.tar.gz -C /usr/share/confluent-hub-components && \
    # Debezium JDBC sink connector (Apache 2.0)
    curl -fsSL "${MAVEN}/io/debezium/debezium-connector-jdbc/${DEBEZIUM_VERSION}/debezium-connector-jdbc-${DEBEZIUM_VERSION}-plugin.tar.gz" -o /tmp/dbz-jdbc.tar.gz && \
    tar -xzf /tmp/dbz-jdbc.tar.gz -C /usr/share/confluent-hub-components && \
    # MySQL JDBC driver for the sink (not bundled, for licensing reasons)
    curl -fsSL "${MAVEN}/com/mysql/mysql-connector-j/${MYSQL_DRIVER_VERSION}/mysql-connector-j-${MYSQL_DRIVER_VERSION}.jar" \
      -o /usr/share/confluent-hub-components/debezium-connector-jdbc/mysql-connector-j-${MYSQL_DRIVER_VERSION}.jar && \
    rm /tmp/dbz-mysql.tar.gz /tmp/dbz-jdbc.tar.gz
USER appuser
