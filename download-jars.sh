#!/bin/bash
# ============================================================
# TaaSim — Download required JARs for S3A connectivity
# Run this ONCE before docker-compose up
# ============================================================

set -e

echo "=== Downloading Spark S3A JARs ==="
mkdir -p jars/spark
cd jars/spark

# hadoop-aws (S3A filesystem implementation)
if [ ! -f hadoop-aws-3.3.4.jar ]; then
  echo "Downloading hadoop-aws-3.3.4.jar..."
  curl -L -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
fi

# AWS SDK bundle (required by hadoop-aws)
if [ ! -f aws-java-sdk-bundle-1.12.367.jar ]; then
  echo "Downloading aws-java-sdk-bundle-1.12.367.jar..."
  curl -L -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar
fi

cd ../..

echo "=== Downloading Flink S3A JARs ==="
mkdir -p jars/flink

cd jars/flink

# Flink S3 filesystem plugin (hadoop-based)
if [ ! -f flink-s3-fs-hadoop-1.18.1.jar ]; then
  echo "Downloading flink-s3-fs-hadoop-1.18.1.jar..."
  curl -L -O https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-hadoop/1.18.1/flink-s3-fs-hadoop-1.18.1.jar
fi

# Flink Kafka connector
if [ ! -f flink-sql-connector-kafka-3.1.0-1.18.jar ]; then
  echo "Downloading flink-sql-connector-kafka-3.1.0-1.18.jar..."
  curl -L -O https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar
fi

# Flink Cassandra connector
if [ ! -f flink-connector-cassandra_2.12-3.1.0-1.17.jar ]; then
  echo "Downloading flink-connector-cassandra..."
  curl -L -O https://repo1.maven.org/maven2/org/apache/flink/flink-connector-cassandra_2.12/3.1.0-1.17/flink-connector-cassandra_2.12-3.1.0-1.17.jar
fi

cd ../..

echo ""
echo "=== All JARs downloaded ==="
echo "Spark JARs:"
ls -la jars/spark/
echo ""
echo "Flink JARs:"
ls -la jars/flink/
echo ""
echo "You can now run: docker-compose up -d"
