#!/usr/bin/env python3
"""
Week 3 Verification Script - TaaSim Pipeline E2E Testing
Tests: Flink job status, Cassandra data flow, Kafka topics, services health
"""

import subprocess
import json
import sys
from datetime import datetime

def run_cmd(cmd):
    """Run shell command and return output"""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=5)
        return result.stdout.strip(), result.returncode
    except subprocess.TimeoutExpired:
        return "", 1
    except Exception as e:
        return f"Error: {e}", 1

def test_flink_jobs():
    """Check Flink job status"""
    print("[1/5] Checking Flink Jobs...")
    cmd = 'docker exec taasim-flink-jm bash -c "curl -s http://localhost:8081/overview"'
    output, code = run_cmd(cmd)
    
    if code == 0 and output:
        try:
            data = json.loads(output)
            print(f"  TaskManagers: {data.get('taskmanagers', 0)}")
            print(f"  Slots: {data.get('slots_available', 0)} available / {data.get('slots_total', 0)} total")
        except:
            print("  (Could not parse Flink response)")
    else:
        print("  (Flink API not responding)")

def test_cassandra():
    """Check Cassandra tables"""
    print("\n[2/5] Checking Cassandra Schema...")
    cmd = 'docker exec taasim-cassandra cqlsh -e "USE taasim; DESCRIBE TABLES;"'
    output, code = run_cmd(cmd)
    
    if code == 0:
        tables = ["vehicle_positions", "trips", "demand_zones"]
        for table in tables:
            if table in output:
                print(f"  [OK] {table}")
            else:
                print(f"  [..] {table}")
    else:
        print("  (Cassandra not responding)")

def test_kafka_topics():
    """Check Kafka topics"""
    print("\n[3/5] Checking Kafka Topics...")
    cmd = 'docker exec taasim-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list'
    output, code = run_cmd(cmd)
    
    if code == 0:
        topics = ["raw.gps", "raw.trips", "processed.gps", "processed.demand", "processed.matches"]
        for topic in topics:
            if topic in output:
                print(f"  [OK] {topic}")
            else:
                print(f"  [..] {topic}")
    else:
        print("  (Kafka not responding)")

def test_cassandra_data():
    """Check Cassandra data population"""
    print("\n[4/5] Checking Cassandra Data...")
    
    queries = {
        "vehicle_positions": "SELECT COUNT(*) as count FROM vehicle_positions;",
        "trips": "SELECT COUNT(*) as count FROM trips;",
        "demand_zones": "SELECT COUNT(*) as count FROM demand_zones;"
    }
    
    for table, query in queries.items():
        cmd = f'docker exec taasim-cassandra cqlsh -e "USE taasim; {query}"'
        output, code = run_cmd(cmd)
        
        if code == 0 and "count" in output:
            # Extract the count value (it's usually after the word "count")
            lines = output.split('\n')
            for line in lines:
                if line.strip() and line.strip() != "count":
                    print(f"  {table}: {line.strip()} rows")
                    break
        else:
            print(f"  {table}: (error querying)")

def test_services():
    """Check Docker service health"""
    print("\n[5/5] Checking Service Health...")
    cmd = 'docker ps --filter "name=taasim" --format "table {{.Names}}\t{{.Status}}"'
    output, code = run_cmd(cmd)
    
    if code == 0:
        services = {
            "taasim-flink-jm": "Flink JobManager",
            "taasim-kafka": "Kafka",
            "taasim-cassandra": "Cassandra",
            "taasim-grafana": "Grafana",
            "taasim-minio": "MinIO"
        }
        
        for container, name in services.items():
            if container in output:
                if "healthy" in output or "running" in output.lower():
                    print(f"  [OK] {name}")
                else:
                    print(f"  [..] {name}")
            else:
                print(f"  [xx] {name} (not found)")
    else:
        print("  (Docker not responding)")

def main():
    print("=" * 60)
    print("TaaSim Week 3 Verification Test")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    test_flink_jobs()
    test_cassandra()
    test_kafka_topics()
    test_cassandra_data()
    test_services()
    
    print("\n" + "=" * 60)
    print("Verification Complete")
    print("Flink Dashboard:  http://localhost:8081")
    print("Grafana:          http://localhost:3000")
    print("=" * 60)

if __name__ == "__main__":
    main()
