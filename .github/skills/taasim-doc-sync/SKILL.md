---
name: taasim-doc-sync
description: Use this skill when docs and implementation may have drift and you need to align README, documents, notebook descriptions, and actual code behavior.
---

# TaaSim Doc Sync

## Trigger
Use this skill when updating docs after code changes, preparing delivery evidence, or auditing conflicting technical claims.

## Workflow
1. Compare README API routes against api/main.py handlers and models.
2. Compare README container/service list against docker-compose.yml.
3. Validate docs claims against Flink and producer code behavior.
4. Reconcile old week documents with documents/00_master_status.md.
5. Compare connector documentation claims with config/kafka-connect/ current files.
6. Produce a drift matrix and exact edits needed per document.

## Required Comparisons
- README routes vs api/main.py
- README container list vs docker-compose.yml
- docs claims vs Flink/producers code
- old Week docs vs current master status
- connector docs vs config/kafka-connect/

## Validation Commands
```bash
rg -n "/api/|GET|POST|PUT|DELETE" README.MD
rg -n "@app\\.(get|post|put|delete)\\(" api/main.py
docker compose config --services
rg -n "raw\\.|processed\\.|watermark|dedup|H3|zone" documents README.MD flink/jobs producers
rg -n "Week|Task|DONE|Upcoming|Status" documents/00_master_status.md documents
dir config/kafka-connect
rg -n "connector|kafka-connect|S3 Sink|DEPRECATED" README.MD documents config/kafka-connect
```

## Output Contract
Return:
1. Drift matrix: claim, source_doc, source_code, status(match/mismatch), fix.
2. Priority-ordered doc update list.
3. Any unresolved ambiguity requiring maintainer decision.
