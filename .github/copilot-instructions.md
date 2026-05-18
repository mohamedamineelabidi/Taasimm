# TaaSim Copilot Instructions

TaaSim is a Casablanca smart mobility simulation platform.

## Architecture Truths
- Kafka is the live event bus.
- Flink is the real-time processor.
- Spark and notebooks are offline-only analysis/training paths.
- Porto and NYC datasets are donor sources only, not live streams.
- Cassandra stores anonymized serving state.

## Always-On Guardrails
- Check documentation claims against current code before updating docs.
- Do not commit large raw datasets or bulky generated data unless explicitly required.
- Keep changes small, verifiable, and aligned with current runtime contracts.
