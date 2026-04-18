FROM python:3.13-slim

WORKDIR /app

RUN pip install --no-cache-dir kafka-python numpy h3 scipy

COPY config.py .
COPY vehicle_gps_producer.py .
COPY trip_request_producer.py .
