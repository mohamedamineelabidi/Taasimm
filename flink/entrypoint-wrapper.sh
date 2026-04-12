#!/bin/bash
# Copy extra JARs into Flink lib at container startup
if [ -d "/opt/flink/lib/extra" ]; then
    for jar in /opt/flink/lib/extra/*.jar; do
        [ -f "$jar" ] && cp -n "$jar" /opt/flink/lib/
    done
    echo "=== Extra JARs copied to /opt/flink/lib/ ==="
fi

# Run the original Flink entrypoint
exec /docker-entrypoint.sh "$@"
