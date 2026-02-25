# Spark Session Operator - Design Context

## Problem
Enable per-user interactive Spark sessions (not communal) for Spark Connect and Spark Thrift Server, 
both deployed as SparkApplications via Spark Operator on K8s.

## Architecture Decisions
- Both Spark Connect and Thrift Server run as SparkApplication CRs (Spark Operator)
- Thin Scala wrapper JARs keep the processes alive (SparkConnectWrapper, SparkThriftWrapper)
- Two CRDs: SparkSessionPool (manages scalable pool) + SparkInteractiveSession (per-user session)
- REST API with OIDC auth for session creation (explicit API, not proxy-based discovery)
- Session-level isolation (in-process, not pod-per-user)
- Pool scales by creating/deleting SparkApplication CRs based on load
- TCP/gRPC proxy for single stable entrypoint routing
- Metrics from Prometheus/Mimir (already exposed via spark-operator-podmonitor)
- Idle timeout: 12h configurable
- Per-user quotas inline in pool spec
- API group: sparkinteractive.io

## Key Specs
- Namespace: spark-dev
- Image registry: fnr-dev-team-docker-repo.repo.corp.tander.ru
- S3: Yandex Cloud (storage.yandexcloud.net)
- Hive Metastore: thrift://hive-metastore.fnr-dev-i.corp.tander.ru:9083
- Delta Lake enabled
- Node selector: dedicated-to-services: spark-i-nosla
