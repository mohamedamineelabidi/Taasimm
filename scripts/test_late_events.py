"""
TaaSim — Late Event Watermark Test
====================================
Sends GPS events with controlled timestamps to verify Flink watermark behavior:
  1. "on-time" event: timestamp = now - 2 minutes (within 3-min watermark)
  2. "late" event:    timestamp = now - 5 minutes (beyond 3-min watermark)

Expected: on-time event appears in Cassandra; late event is dropped.
"""

import json
import time
import sys
import os
from datetime import datetime, timezone, timedelta
from kafka import KafkaProducer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "raw.gps"

# Casablanca center coordinates (zone 9 - Maarif)
TEST_LAT = 33.575
TEST_LON = -7.625


def make_event(taxi_id, timestamp_dt, lat, lon, speed=25.0, status="moving"):
    ts_epoch = int(timestamp_dt.timestamp())
    return {
        "taxi_id": taxi_id,
        "trip_id": f"TEST-{taxi_id}",
        "timestamp": ts_epoch,
        "event_time": timestamp_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "lat": lat,
        "lon": lon,
        "speed_kmh": speed,
        "status": status,
    }


def main():
    print("=== TaaSim Late Event Watermark Test ===\n")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    now = datetime.now(timezone.utc)

    # Event 1: on-time (2 min ago — within 3-min watermark tolerance)
    on_time_ts = now - timedelta(minutes=2)
    on_time_event = make_event("TEST-ONTIME-001", on_time_ts, TEST_LAT, TEST_LON)
    print(f"[1] Sending ON-TIME event (now - 2min): taxi={on_time_event['taxi_id']}")
    print(f"    timestamp={on_time_event['event_time']}")
    producer.send(TOPIC, value=on_time_event)
    producer.flush()
    print("    -> Sent to raw.gps\n")

    time.sleep(2)

    # Event 2: late (5 min ago — beyond 3-min watermark tolerance)
    late_ts = now - timedelta(minutes=5)
    late_event = make_event("TEST-LATE-001", late_ts, TEST_LAT + 0.01, TEST_LON + 0.01)
    print(f"[2] Sending LATE event (now - 5min): taxi={late_event['taxi_id']}")
    print(f"    timestamp={late_event['event_time']}")
    producer.send(TOPIC, value=late_event)
    producer.flush()
    print("    -> Sent to raw.gps\n")

    producer.close()

    print("=" * 50)
    print("Verify results after ~30s:")
    print(f"  ON-TIME (taxi_id='TEST-ONTIME-001') should appear in Cassandra")
    print(f"  LATE    (taxi_id='TEST-LATE-001')    should be DROPPED by watermark")
    print()
    print("Check with:")
    print("  docker exec taasim-cassandra cqlsh -e \"SELECT taxi_id, zone_id, event_time, lat, lon FROM taasim.vehicle_positions WHERE city='casablanca' AND zone_id=9 LIMIT 10;\"")
    print()
    print("Also check processed.gps topic:")
    print("  docker exec taasim-kafka /opt/kafka/bin/kafka-console-consumer.sh \\")
    print("    --bootstrap-server localhost:9092 --topic processed.gps \\")
    print("    --from-beginning --max-messages 50 --timeout-ms 10000 | findstr TEST")


if __name__ == "__main__":
    main()
