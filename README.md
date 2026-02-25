# Spark Session Operator

A Kubernetes operator that manages pools of interactive Spark servers (Spark Connect and Spark Thrift Server) and provides per-user session management with OIDC authentication, autoscaling, and idle timeout.

## Architecture

The operator manages two CRDs:

- **SparkSessionPool** — a scalable pool of SparkApplication instances (Spark Connect or Thrift Server). The controller creates/deletes SparkApplication CRs via the Spark Operator based on load.
- **SparkInteractiveSession** — a per-user session assignment. The controller assigns users to the least-loaded pool instance, enforces quotas, and handles idle timeouts.

Additional components running in the same binary:

- **REST API Gateway** (`:8080`) — OIDC-authenticated HTTP API for listing/deleting sessions
- **Thrift Proxy** (`:10009`) — SASL PLAIN-authenticated proxy that auto-creates sessions for Thrift (HiveServer2) connections
- **Connect Proxy** (`:15002`) — gRPC proxy that auto-creates sessions for Spark Connect connections
- **Metrics Client** — queries the Kubernetes Metrics API (`metrics.k8s.io`) for CPU/memory-based scaling

### Session creation paths

| Path | Protocol | Use case |
|------|----------|----------|
| **Proxy** (primary) | Thrift SASL / gRPC metadata | Interactive users from Jupyter, DBeaver, PySpark |
| **kubectl / K8s API** | SparkInteractiveSession CR | Automation / CI pipelines |
| **REST API** | HTTP (read + delete only) | List sessions, get details, terminate sessions |

Users connect with their domain credentials (Keycloak username/password). The proxy exchanges credentials for a JWT via Keycloak's ROPC grant, auto-selects the pool, creates a session, waits for it to become active, and proxies traffic transparently. Keepalive is handled automatically while the connection is open.

## Prerequisites

- Kubernetes cluster v1.28+
- [Spark Operator](https://github.com/kubeflow/spark-operator) installed (manages SparkApplication CRs)
- [Metrics Server](https://github.com/kubernetes-sigs/metrics-server) installed (required for `cpu`/`memory` scaling; not needed for `activeSessions`)
- [Keycloak](https://www.keycloak.org/) or compatible OIDC provider (with ROPC grant enabled for the proxy client)
- `kubectl` configured to access your cluster
- Go 1.23+ (for building from source)
- Docker (for building the container image)

## Quick Start

### 1. Install CRDs

```sh
make install
```

This installs the `SparkSessionPool` and `SparkInteractiveSession` CRDs into your cluster.

### 2. Build and push the image

```sh
export IMG=your-registry.example.com/spark-session-operator:latest
make docker-build docker-push IMG=$IMG
```

### 3. Deploy the operator

```sh
make deploy IMG=$IMG
```

This deploys the controller manager, service account, RBAC roles, and CRDs into the `spark-session-operator` namespace.

### 4. Create a SparkSessionPool

Each pool manages a set of identical Spark server instances of a given type. The proxy auto-selects the pool by type — there must be exactly one pool of each type (`connect` or `thrift`) in the namespace.

Apply one of the sample pools:

```sh
# Spark Connect pool
kubectl apply -f config/samples/connect-pool.yaml

# Spark Thrift Server pool
kubectl apply -f config/samples/thrift-pool.yaml
```

A pool defines three things:

**1. Server type and replicas**

```yaml
spec:
  type: connect        # "connect" (Spark Connect gRPC) or "thrift" (HiveServer2)
  replicas:
    min: 1             # Always keep at least this many instances running
    max: 5             # Never scale beyond this
```

**2. SparkApplication template** — the full Spark Operator `SparkApplication` spec used to create each instance. This is where you configure the Spark image, driver/executor resources, Hive metastore, S3, Delta Lake, etc. The operator creates one `SparkApplication` CR per pool instance using this template.

**3. Session policy** — controls per-user limits and idle cleanup:

```yaml
spec:
  sessionPolicy:
    maxSessionsPerUser: 5       # Max concurrent sessions per user
    maxTotalSessions: 200       # Max sessions across the pool
    idleTimeoutMinutes: 720     # Kill sessions idle for 12+ hours
    defaultSessionConf:         # Default Spark configs applied to each session
      spark.executor.memory: "4g"
    quotas:                     # Per-user/group overrides
      - match:
          users: ["admin"]
        maxSessionsPerUser: 20
        sessionConf:
          spark.dynamicAllocation.maxExecutors: "20"
```

Verify the pool is running:

```sh
kubectl get sparksessionpools -n spark-dev
# NAME           TYPE      REPLICAS   READY   SESSIONS   AGE
# connect-pool   connect   1          1       0          5m
```

### 5. Configure Keycloak (for proxy auth)

The proxy authenticates users via Keycloak's Resource Owner Password Credentials (ROPC) grant. Skip this step if you're only creating sessions via `kubectl`.

1. In your Keycloak realm, create a new client (e.g. `spark-session-operator`)
2. Set **Client authentication** to `On` (confidential) or `Off` (public), depending on your security requirements
3. Under **Authentication flow overrides**, enable **Direct access grants** (this enables the ROPC flow)
4. Note the **Client ID** and **Client secret** (if confidential)

Then pass the credentials to the operator:

```sh
--oidc-issuer-url=https://keycloak.example.com/realms/spark \
--oidc-client-id=spark-session-operator \
--oidc-client-secret=<client-secret>   # omit if public client
```

Users will authenticate with their regular Keycloak username and password — the proxy exchanges these for a JWT automatically.

### 6. Connect via proxy

**DBeaver (Thrift):**
1. Create a new Apache Hive connection
2. Host: `<proxy-endpoint>`, Port: `10009`
3. Authentication: enter your domain username and password
4. A session is auto-created when you connect

**PyHive (Thrift):**
```python
from pyhive import hive

conn = hive.connect(
    host='<proxy-endpoint>',
    port=10009,
    auth='CUSTOM',
    username='alice',
    password='my-domain-password',
)
```

**PySpark (Spark Connect):**
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .remote("sc://<proxy-endpoint>:15002") \
    .config("spark.connect.grpc.metadata", "x-spark-username=alice,x-spark-password=my-domain-password") \
    .getOrCreate()
```

Or apply a session CR directly (for automation):

```sh
kubectl apply -f config/samples/session.yaml
```

When creating sessions via `kubectl apply`, you don't need Keycloak or the proxy — connect directly to the backend. First, get the session endpoint:

```sh
kubectl get sparkinteractivesession <session-name> -n spark-dev -o jsonpath='{.status.endpoint}'
```

Then port-forward to the assigned instance:

```sh
kubectl port-forward pod/<assigned-instance-driver> -n spark-dev 15002:15002
```

And connect with PySpark without credentials:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .remote("sc://localhost:15002") \
    .getOrCreate()

spark.sql("SELECT 1").show()
```

## Configuration

The operator accepts the following command-line flags:

| Flag | Default | Description |
|------|---------|-------------|
| `--namespace` | `spark-dev` | Namespace to watch and manage resources in |
| `--gateway-addr` | `:8080` | Address for the REST API gateway |
| `--thrift-proxy-addr` | `:10009` | Address for the Thrift proxy |
| `--connect-proxy-addr` | `:15002` | Address for the Spark Connect gRPC proxy |
| `--oidc-issuer-url` | _(empty)_ | OIDC issuer URL (e.g. `https://keycloak.example.com/realms/spark`) |
| `--oidc-audience` | _(empty)_ | Expected OIDC audience |
| `--oidc-user-claim` | `sub` | JWT claim containing the username |
| `--oidc-groups-claim` | `groups` | JWT claim containing user groups |
| `--oidc-skip-validation` | `false` | Skip OIDC token validation (dev only) |
| `--oidc-client-id` | _(empty)_ | OAuth client ID for Keycloak ROPC grant (used by proxy) |
| `--oidc-client-secret` | _(empty)_ | OAuth client secret for Keycloak ROPC grant (optional) |
| `--leader-elect` | `false` | Enable leader election for HA |
| `--health-probe-bind-address` | `:8081` | Health/readiness probe address |
| `--metrics-bind-address` | `0` | Metrics endpoint address (`0` = disabled) |
| `--metrics-secure` | `true` | Serve metrics over HTTPS |

## REST API

All endpoints require an `Authorization: Bearer <token>` header.

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/sessions` | List your sessions |
| `GET` | `/api/v1/sessions/{name}` | Get session details |
| `DELETE` | `/api/v1/sessions/{name}` | Terminate a session |

Unauthenticated health endpoints:

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/healthz` | Liveness probe |
| `GET` | `/readyz` | Readiness probe |

### Session response

Returned by GET and DELETE endpoints.

```json
{
  "name": "session-alice-12345",
  "user": "alice",
  "pool": "connect-pool",
  "state": "Active",
  "assignedInstance": "connect-pool-54321",
  "createdAt": "2026-02-25T10:00:00Z",
  "lastActivityAt": "2026-02-25T10:05:00Z"
}
```

## CRD Reference

### SparkSessionPool

```yaml
apiVersion: sparkinteractive.io/v1alpha1
kind: SparkSessionPool
metadata:
  name: connect-pool
  namespace: spark-dev
spec:
  type: connect                    # "connect" or "thrift"
  replicas:
    min: 1                         # Minimum running instances
    max: 5                         # Maximum instances
  scaling:
    metrics:
      type: activeSessions         # activeSessions | cpu | memory
      targetPerInstance: 20         # See target semantics below
    scaleUpThreshold: "0.8"        # Scale up when load > 80% of target (activeSessions only)
    scaleDownThreshold: "0.3"      # Scale down when load < 30% of target
    cooldownSeconds: 300           # Min seconds between scaling actions
    drainBeforeScaleDown: true     # Drain sessions before removing instance
  sparkApplicationTemplate:
    spec: { ... }                  # Raw SparkApplication spec (passed to Spark Operator)
  sessionPolicy:
    maxSessionsPerUser: 5
    maxTotalSessions: 200
    idleTimeoutMinutes: 720        # 12 hours
    defaultSessionConf:
      spark.executor.memory: "4g"
    quotas:                        # Per-user/group overrides
      - match:
          users: ["admin"]
        maxSessionsPerUser: 20
```

#### Scaling metrics

The `spec.scaling.metrics.type` field selects a single metric for autoscaling. Only one metric type is active per pool.

| Type | `targetPerInstance` meaning | Default target | Data source |
|------|----------------------------|----------------|-------------|
| `activeSessions` | Target session count per instance | 20 | SparkInteractiveSession CRs |
| `cpu` | Target CPU utilization % (0–100) | 80 | Kubernetes Metrics API (`metrics.k8s.io`) |
| `memory` | Target memory utilization % (0–100) | 80 | Kubernetes Metrics API (`metrics.k8s.io`) |

For `cpu` and `memory`, the controller reads driver pod resource usage from the Metrics API and compares it against resource requests. The desired replica count follows the HPA formula: `ceil(currentReplicas * avgUtilization / target)`. This requires [Metrics Server](https://github.com/kubernetes-sigs/metrics-server) (or an equivalent `metrics.k8s.io` provider) to be installed in the cluster.

For `activeSessions`, the controller counts active/idle SparkInteractiveSession CRs assigned to each instance. The `scaleUpThreshold` adds headroom: if the average load per instance exceeds `target * scaleUpThreshold`, an extra replica is added.

### SparkInteractiveSession

```yaml
apiVersion: sparkinteractive.io/v1alpha1
kind: SparkInteractiveSession
metadata:
  name: session-alice-12345
  namespace: spark-dev
spec:
  user: alice
  pool: connect-pool
  sparkConf:
    spark.executor.memory: "8g"
```

## Development

### Run locally (outside cluster)

```sh
# Install CRDs
make install

# Run the operator against your current kubeconfig
make run
```

### Build

```sh
# Generate deepcopy and CRD manifests
make generate manifests

# Build the binary
CGO_ENABLED=0 go build -o bin/manager cmd/main.go
```

### Run tests

```sh
make test
```

### Project structure

```
├── api/v1alpha1/                  # CRD type definitions
│   ├── groupversion_info.go
│   ├── sparksessionpool_types.go
│   ├── sparkinteractivesession_types.go
│   └── zz_generated.deepcopy.go   # Generated
├── cmd/main.go                    # Entrypoint
├── internal/
│   ├── auth/token.go              # Shared OIDC auth + Keycloak ROPC
│   ├── controller/                # Reconciliation logic
│   │   ├── sparksessionpool_controller.go
│   │   └── sparkinteractivesession_controller.go
│   ├── gateway/server.go          # REST API gateway (read + delete)
│   └── proxy/                     # Auto-session proxies
│       ├── proxy.go               # Session lifecycle + keepalive
│       ├── thrift_sasl.go         # SASL PLAIN frame parser
│       └── connect_grpc.go        # gRPC metadata extraction + raw codec
├── config/
│   ├── crd/bases/                 # Generated CRD YAMLs
│   ├── manager/manager.yaml       # Deployment manifest
│   ├── rbac/                      # Generated RBAC from markers
│   └── samples/                   # Example CRs
├── Dockerfile
├── Makefile
└── PROJECT                        # Kubebuilder project metadata
```

## Uninstall

```sh
# Remove CRs
kubectl delete -k config/samples/

# Undeploy the operator
make undeploy

# Remove CRDs
make uninstall
```

## License

Copyright 2026 Tander.

Licensed under the Apache License, Version 2.0.
