FROM flink:1.18-scala_2.12-java11

# Install Python 3 + pip
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip python3-dev && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# Install PyFlink and Python dependencies
RUN pip3 install --no-cache-dir \
    apache-flink==1.18.1 \
    cassandra-driver==3.29.2 \
    kafka-python==2.0.2

# Copy extra JARs into Flink's lib so they auto-load
# (mounted at /opt/flink/lib/extra via docker-compose volume)
COPY entrypoint-wrapper.sh /opt/flink/entrypoint-wrapper.sh
RUN chmod +x /opt/flink/entrypoint-wrapper.sh

USER flink
