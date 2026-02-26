# Spark Session Operator

A Kubernetes operator that manages pools of interactive Spark servers (Spark Connect and Spark Thrift Server) and provides per-user session management with OIDC authentication, autoscaling, and idle timeout.

## Architecture

![Architecture](docs/architecture.png)

> Diagram source: [`docs/architecture.dot`](docs/architecture.dot). Regenerate with `dot -Tpng docs/architecture.dot -o docs/architecture.png -Gdpi=150`.

The operator manages two CRDs:

- **SparkSessionPool** — a scalable pool of SparkApplication instances (Spark Connect or Thrift Server). The controller creates/deletes SparkApplication CRs via the Spark Operator based on load. Each pool has its own hostname and a dynamically created Ingress for routing.
- **SparkInteractiveSession** — a per-user session assignment. The controller assigns users to the least-loaded pool instance, enforces quotas, and handles idle timeouts.

Additional components running in the same binary:

- **REST API Gateway** (`:8080`) — OIDC-authenticated HTTP API for listing/deleting sessions
- **Thrift HTTP Proxy** (`:10009`) — HTTP reverse proxy with Basic auth (Keycloak ROPC) that auto-creates sessions for Thrift (HiveServer2 HTTP transport) connections
- **Connect gRPC Proxy** (`:15002`) — gRPC proxy that auto-creates sessions for Spark Connect connections
- **Metrics Client** — queries the Kubernetes Metrics API (`metrics.k8s.io`) for CPU/memory-based scaling

### Hostname-based routing

Each pool defines a `spec.host` field (e.g. `spark-connect-default.example.com`). The pool controller automatically creates an Ingress per pool in the operator's namespace, routing traffic to the appropriate proxy port:

| Pool type | Ingress backend | Protocol | Port |
|-----------|----------------|----------|------|
| `connect` | gRPC proxy | GRPC | 15002 |
| `thrift`  | Thrift HTTP proxy | HTTP | 10009 |

This allows multiple pools of the same type (e.g. `connect-default-pool` and `connect-heavy-pool`) to coexist. The proxy selects the target pool by hostname: the Thrift HTTP proxy uses the standard `Host` header (preserved by nginx for HTTP backends), while the Connect gRPC proxy reads `X-Forwarded-Host` from gRPC metadata (nginx ingress automatically sets `grpc_set_header x-forwarded-host` for gRPC backends).

### Scale from zero

Pools support `replicas.min: 0`. When a user connects and no instances exist, the proxy creates a session CR in `Pending` state. The pool controller counts pending unassigned sessions alongside active ones, triggering SparkApplication creation even when the pool has zero running instances.

### Session creation paths

| Path | Protocol | Use case |
|------|----------|----------|
| **Proxy** (primary) | Thrift HTTP / gRPC | Interactive users from Jupyter, DBeaver, PySpark |
| **kubectl / K8s API** | SparkInteractiveSession CR | Automation / CI pipelines |
| **REST API** | HTTP (read + delete only) | List sessions, get details, terminate sessions |

Users connect with their domain credentials (Keycloak username/password). The proxy exchanges credentials for a JWT via Keycloak's ROPC grant, selects the pool by hostname, creates a session, waits for it to become active, and proxies traffic transparently. Keepalive is handled automatically while the connection is open.

## Prerequisites

- Kubernetes cluster v1.28+
- [Spark Operator](https://github.com/kubeflow/spark-operator) installed (manages SparkApplication CRs)
- [Metrics Server](https://github.com/kubernetes-sigs/metrics-server) installed (required for `cpu`/`memory` scaling; not needed for `activeSessions`)
- [Keycloak](https://www.keycloak.org/) or compatible OIDC provider (with ROPC grant enabled for the proxy client)
- Nginx Ingress Controller (for dynamic per-pool Ingress routing)
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

### 4. Create SparkSessionPools

Each pool manages a set of identical Spark server instances. Pools are identified by hostname — you can have multiple pools of the same type with different resource profiles (e.g. `connect-default-pool` and `connect-heavy-pool`).

Apply the sample pools:

```sh
# Spark Connect pools (default + heavy)
kubectl apply -f config/samples/connect-pool.yaml
kubectl apply -f config/samples/connect-heavy-pool.yaml

# Spark Thrift Server pools
kubectl apply -f config/samples/thrift-pool.yaml
kubectl apply -f config/samples/thrift-heavy-pool.yaml
```

A pool defines three things:

**1. Server type, hostname, and replicas**

```yaml
spec:
  type: connect                                      # "connect" or "thrift"
  host: spark-connect-default.example.com            # Hostname for Ingress routing
  replicas:
    min: 0             # Min instances (0 = scale from zero)
    max: 5             # Maximum instances
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

Verify the pools are created:

```sh
kubectl get sparksessionpools -n spark-dev
# NAME                   TYPE      REPLICAS   READY   SESSIONS   AGE
# connect-default-pool   connect   0          0       0          5m
# connect-heavy-pool     connect   0          0       0          5m
# thrift-default-pool    thrift    0          0       0          5m
```

The operator automatically creates an Ingress for each pool. Verify:

```sh
kubectl get ingress -n spark-session-operator
# NAME                          HOSTS                                   ...
# connect-default-pool-connect  spark-connect-default.example.com       ...
# connect-heavy-pool-connect    spark-connect-heavy.example.com         ...
# thrift-default-pool-thrift    spark-thrift-default.example.com        ...
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

For development or testing without a Keycloak instance, you can skip token validation entirely:

```sh
--oidc-skip-validation=true
```

In this mode the proxy accepts any username/password without contacting an OIDC provider. The username is taken as-is for session ownership. **Do not use this in production.**

### 6. Connect via proxy

**DBeaver (Thrift via HTTP transport):**
1. Create a new Apache Hive connection
2. JDBC URL: `jdbc:hive2://spark-thrift-default.example.com:80/default;transportMode=http;httpPath=cliservice`
3. Authentication: enter your domain username and password
4. A session is auto-created when you connect

**PyHive (Thrift via HTTP transport):**
```python
from pyhive import hive

conn = hive.connect(
    host='spark-thrift-default.example.com',
    port=80,
    auth='CUSTOM',
    username='alice',
    password='my-domain-password',
    thrift_transport=hive.Transport.HTTP,
    http_path='cliservice',
)
```

**PySpark (Spark Connect):**
```python
from pyspark.sql import SparkSession
import base64

token = base64.b64encode(b"alice:my-domain-password").decode()

spark = SparkSession.builder \
    .remote("sc://spark-connect-default.example.com:443/;token=" + token) \
    .getOrCreate()

spark.sql("SELECT 1").show()
```

Or with explicit gRPC metadata (insecure channel):
```python
spark = SparkSession.builder \
    .remote("sc://spark-connect-default.example.com:80/;user_id=" + token) \
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
# Spark Connect (port 8424 inside the pod)
kubectl port-forward pod/<assigned-instance-driver> -n spark-dev 15002:8424

# Spark Thrift Server (port 10001 inside the pod, HTTP transport)
kubectl port-forward pod/<assigned-instance-driver> -n spark-dev 10009:10001
```

## Configuration

The operator accepts the following command-line flags:

| Flag | Default | Description |
|------|---------|-------------|
| `--namespace` | `spark-dev` | Namespace to watch and manage resources in |
| `--proxy-namespace` | `spark-session-operator` | Namespace where the proxy Service and Ingresses live |
| `--gateway-addr` | `:8080` | Address for the REST API gateway |
| `--thrift-proxy-addr` | `:10009` | Address for the Thrift HTTP proxy |
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

All endpoints require an `Authorization: Bearer <token>` header with a Keycloak JWT.

**For users** — open this link in a browser, log in with your domain credentials, and copy the token from the response:
```
https://keycloak.example.com/realms/YourRealm/protocol/openid-connect/auth
  ?client_id=spark-session-operator
  &redirect_uri=https://spark-api.example.com/callback
  &response_type=code
  &scope=openid+profile+email
```

**For scripts / CI** — obtain the token programmatically via curl:
```sh
TOKEN=$(curl -s -X POST \
  "https://keycloak.example.com/realms/YourRealm/protocol/openid-connect/token" \
  -d "grant_type=password&client_id=spark-session-operator" \
  -d "username=alice&password=my-password" | jq -r .access_token)

curl -H "Authorization: Bearer $TOKEN" \
  https://spark-api.example.com/api/v1/pools
```

With `--oidc-skip-validation` enabled, any valid-looking JWT is accepted without signature verification.

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/sessions` | List your sessions |
| `GET` | `/api/v1/sessions/{name}` | Get session details |
| `DELETE` | `/api/v1/sessions/{name}` | Terminate a session |

Unauthenticated endpoints (no token required):

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/pools` | List all pools (HTML table in browser, JSON for programmatic access; `?format=json` forces JSON) |
| `GET` | `/healthz` | Liveness probe |
| `GET` | `/readyz` | Readiness probe |

### Pool response

Returned by `GET /api/v1/pools`.

```json
[
  {
    "name": "connect-default-pool",
    "type": "connect",
    "host": "spark-connect-default.example.com",
    "minReplicas": 0,
    "maxReplicas": 5,
    "currentReplicas": 2,
    "readyReplicas": 2,
    "totalActiveSessions": 3,
    "sessionPolicy": {
      "maxSessionsPerUser": 5,
      "maxTotalSessions": 200,
      "idleTimeoutMinutes": 720,
      "defaultSessionConf": {
        "spark.executor.memory": "4g"
      }
    }
  }
]
```

### Session response

Returned by `GET` and `DELETE` session endpoints.

```json
{
  "name": "session-alice-12345",
  "user": "alice",
  "pool": "connect-default-pool",
  "state": "Active",
  "assignedInstance": "connect-default-pool-54321",
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
  name: connect-default-pool
  namespace: spark-dev
spec:
  type: connect                    # "connect" or "thrift"
  host: spark-connect-default.example.com  # Hostname for Ingress routing
  replicas:
    min: 0                         # Minimum instances (0 = scale from zero)
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

For `activeSessions`, the controller counts active/idle SparkInteractiveSession CRs assigned to each instance, plus pending sessions not yet assigned (for scale-from-zero). The `scaleUpThreshold` adds headroom: if the average load per instance exceeds `target * scaleUpThreshold`, an extra replica is added.

### SparkInteractiveSession

```yaml
apiVersion: sparkinteractive.io/v1alpha1
kind: SparkInteractiveSession
metadata:
  name: session-alice-12345
  namespace: spark-dev
spec:
  user: alice
  pool: connect-default-pool
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

### Regenerate architecture diagram

Requires [Graphviz](https://graphviz.org/):

```sh
dot -Tpng docs/architecture.dot -o docs/architecture.png -Gdpi=150
dot -Tsvg docs/architecture.dot -o docs/architecture.svg
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
│   │   ├── sparksessionpool_controller.go      # Pool scaling, Ingress, lifecycle
│   │   └── sparkinteractivesession_controller.go  # Session assignment, timeout
│   ├── gateway/server.go          # REST API gateway (read + delete)
│   └── proxy/                     # Auto-session proxies
│       ├── proxy.go               # Session lifecycle, Thrift HTTP proxy, Connect gRPC proxy
│       └── connect_grpc.go        # gRPC credential extraction + raw codec
├── config/
│   ├── crd/bases/                 # Generated CRD YAMLs
│   ├── default/                   # Kustomize overlays (ingress, service, patches)
│   ├── manager/manager.yaml       # Deployment manifest
│   ├── rbac/                      # Generated RBAC from markers
│   └── samples/                   # Example pool and session CRs
├── docs/
│   ├── architecture.dot           # Graphviz source for architecture diagram
│   ├── architecture.png           # Rendered diagram (PNG)
│   └── architecture.svg           # Rendered diagram (SVG)
├── scripts/
│   └── test_thrift.py             # PyHive test script for Thrift HTTP transport
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
