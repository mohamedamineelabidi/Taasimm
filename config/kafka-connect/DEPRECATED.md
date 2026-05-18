# DEPRECATED — do not use

The `s3-sink-connector.json` file in this directory was a unified-topic config
auto-registered by the now-removed `connect-init` docker-compose service.

The TaaSim canonical S3 archival uses **two split connectors**:

- `config/connect-s3-sink-gps.json`   → topic `raw.gps`
- `config/connect-s3-sink-trips.json` → topic `raw.trips`

Both are registered manually via:

```powershell
.\scripts\register-connectors.ps1
```

See `README.MD` §7 for full instructions.

You can safely delete `s3-sink-connector.json` and this folder. They are
retained only to prevent surprise on existing clones.
