# ============================================================
# TaaSim — Download required JARs for S3A connectivity
# Run this ONCE before docker-compose up (Windows PowerShell)
# ============================================================

$ErrorActionPreference = "Stop"

Write-Host "=== Downloading Spark S3A JARs ===" -ForegroundColor Cyan

New-Item -ItemType Directory -Force -Path "jars\spark" | Out-Null
Push-Location "jars\spark"

# hadoop-aws
if (-not (Test-Path "hadoop-aws-3.3.4.jar")) {
    Write-Host "Downloading hadoop-aws-3.3.4.jar..."
    Invoke-WebRequest -Uri "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" -OutFile "hadoop-aws-3.3.4.jar"
}

# AWS SDK bundle
if (-not (Test-Path "aws-java-sdk-bundle-1.12.367.jar")) {
    Write-Host "Downloading aws-java-sdk-bundle-1.12.367.jar..."
    Invoke-WebRequest -Uri "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar" -OutFile "aws-java-sdk-bundle-1.12.367.jar"
}

Pop-Location

Write-Host "=== Downloading Flink JARs ===" -ForegroundColor Cyan

New-Item -ItemType Directory -Force -Path "jars\flink" | Out-Null
Push-Location "jars\flink"

# Flink S3 filesystem plugin
if (-not (Test-Path "flink-s3-fs-hadoop-1.18.1.jar")) {
    Write-Host "Downloading flink-s3-fs-hadoop-1.18.1.jar..."
    Invoke-WebRequest -Uri "https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-hadoop/1.18.1/flink-s3-fs-hadoop-1.18.1.jar" -OutFile "flink-s3-fs-hadoop-1.18.1.jar"
}

# Flink Kafka connector
if (-not (Test-Path "flink-sql-connector-kafka-3.1.0-1.18.jar")) {
    Write-Host "Downloading flink-sql-connector-kafka-3.1.0-1.18.jar..."
    Invoke-WebRequest -Uri "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar" -OutFile "flink-sql-connector-kafka-3.1.0-1.18.jar"
}

# Flink Cassandra connector
if (-not (Test-Path "flink-connector-cassandra_2.12-3.1.0-1.17.jar")) {
    Write-Host "Downloading flink-connector-cassandra..."
    Invoke-WebRequest -Uri "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-cassandra_2.12/3.1.0-1.17/flink-connector-cassandra_2.12-3.1.0-1.17.jar" -OutFile "flink-connector-cassandra_2.12-3.1.0-1.17.jar"
}

Pop-Location

Write-Host ""
Write-Host "=== All JARs downloaded ===" -ForegroundColor Green
Write-Host "Spark JARs:"
Get-ChildItem "jars\spark" | Format-Table Name, Length
Write-Host "Flink JARs:"
Get-ChildItem "jars\flink" | Format-Table Name, Length
Write-Host "You can now run: docker-compose up -d" -ForegroundColor Yellow
