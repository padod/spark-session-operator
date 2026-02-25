# Spark Session Operator

Kubernetes-оператор для управления пулами интерактивных Spark-серверов (Spark Connect и Spark Thrift Server) с поддержкой управления сессиями на уровне пользователей, OIDC-аутентификацией, автоскейлингом и таймаутом неактивности.

## Архитектура

Оператор управляет двумя CRD:

- **SparkSessionPool** — масштабируемый пул экземпляров SparkApplication (Spark Connect или Thrift Server). Контроллер создаёт и удаляет SparkApplication CR через Spark Operator в зависимости от нагрузки.
- **SparkInteractiveSession** — привязка сессии к конкретному пользователю. Контроллер назначает пользователей на наименее загруженный экземпляр пула, контролирует квоты и обрабатывает таймауты неактивности.

Дополнительные компоненты, работающие в том же бинарном файле:

- **REST API Gateway** (`:8080`) — HTTP API с OIDC-аутентификацией для просмотра и удаления сессий
- **Thrift Proxy** (`:10009`) — прокси с SASL PLAIN-аутентификацией, автоматически создающий сессии для Thrift (HiveServer2) подключений
- **Connect Proxy** (`:15002`) — gRPC-прокси, автоматически создающий сессии для Spark Connect подключений
- **Metrics Client** — опрашивает Kubernetes Metrics API (`metrics.k8s.io`) для скейлинга по CPU/памяти

### Пути создания сессий

| Путь | Протокол | Сценарий использования |
|------|----------|----------------------|
| **Proxy** (основной) | Thrift SASL / gRPC metadata | Интерактивные пользователи из Jupyter, DBeaver, PySpark |
| **kubectl / K8s API** | SparkInteractiveSession CR | Автоматизация / CI-пайплайны |
| **REST API** | HTTP (только чтение + удаление) | Просмотр сессий, получение деталей, завершение сессий |

Пользователи подключаются с доменными учётными данными (логин/пароль Keycloak). Прокси обменивает учётные данные на JWT через Keycloak ROPC grant, автоматически выбирает пул, создаёт сессию, ожидает её активации и прозрачно проксирует трафик. Keepalive поддерживается автоматически, пока соединение открыто.

## Требования

- Kubernetes-кластер v1.28+
- Установленный [Spark Operator](https://github.com/kubeflow/spark-operator) (управляет SparkApplication CR)
- Установленный [Metrics Server](https://github.com/kubernetes-sigs/metrics-server) (необходим для скейлинга по `cpu`/`memory`; не нужен для `activeSessions`)
- [Keycloak](https://www.keycloak.org/) или совместимый OIDC-провайдер (с включённым ROPC grant для клиента прокси)
- `kubectl`, настроенный для доступа к кластеру
- Go 1.23+ (для сборки из исходников)
- Docker (для сборки контейнерного образа)

## Быстрый старт

### 1. Установка CRD

```sh
make install
```

Устанавливает CRD `SparkSessionPool` и `SparkInteractiveSession` в кластер.

### 2. Сборка и публикация образа

```sh
export IMG=your-registry.example.com/spark-session-operator:latest
make docker-build docker-push IMG=$IMG
```

### 3. Деплой оператора

```sh
make deploy IMG=$IMG
```

Разворачивает controller manager, service account, RBAC-роли и CRD в namespace `spark-session-operator`.

### 4. Создание SparkSessionPool

Каждый пул управляет набором идентичных экземпляров Spark-сервера определённого типа. Прокси автоматически выбирает пул по типу — в namespace должен быть ровно один пул каждого типа (`connect` или `thrift`).

Примените один из примеров пулов:

```sh
# Пул Spark Connect
kubectl apply -f config/samples/connect-pool.yaml

# Пул Spark Thrift Server
kubectl apply -f config/samples/thrift-pool.yaml
```

Пул определяет три вещи:

**1. Тип сервера и количество реплик**

```yaml
spec:
  type: connect        # "connect" (Spark Connect gRPC) или "thrift" (HiveServer2)
  replicas:
    min: 1             # Минимальное количество всегда запущенных экземпляров
    max: 5             # Максимальное количество экземпляров
```

**2. Шаблон SparkApplication** — полная спецификация `SparkApplication` для Spark Operator, используемая при создании каждого экземпляра. Здесь настраиваются Spark-образ, ресурсы driver/executor, Hive metastore, S3, Delta Lake и т.д. Оператор создаёт один `SparkApplication` CR на каждый экземпляр пула по этому шаблону.

**3. Политика сессий** — управление лимитами на пользователя и очисткой неактивных сессий:

```yaml
spec:
  sessionPolicy:
    maxSessionsPerUser: 5       # Максимум одновременных сессий на пользователя
    maxTotalSessions: 200       # Максимум сессий во всём пуле
    idleTimeoutMinutes: 720     # Завершение сессий, неактивных более 12 часов
    defaultSessionConf:         # Конфигурация Spark по умолчанию для каждой сессии
      spark.executor.memory: "4g"
    quotas:                     # Переопределения для отдельных пользователей/групп
      - match:
          users: ["admin"]
        maxSessionsPerUser: 20
        sessionConf:
          spark.dynamicAllocation.maxExecutors: "20"
```

Проверка состояния пула:

```sh
kubectl get sparksessionpools -n spark-dev
# NAME           TYPE      REPLICAS   READY   SESSIONS   AGE
# connect-pool   connect   1          1       0          5m
```

### 5. Настройка Keycloak (для аутентификации через прокси)

Прокси аутентифицирует пользователей через Keycloak Resource Owner Password Credentials (ROPC) grant. Пропустите этот шаг, если сессии создаются только через `kubectl`.

1. В вашем realm Keycloak создайте нового клиента (например, `spark-session-operator`)
2. Установите **Client authentication** в `On` (confidential) или `Off` (public), в зависимости от требований безопасности
3. В **Authentication flow overrides** включите **Direct access grants** (это активирует ROPC flow)
4. Запишите **Client ID** и **Client secret** (если confidential)

Затем передайте учётные данные оператору:

```sh
--oidc-issuer-url=https://keycloak.example.com/realms/spark \
--oidc-client-id=spark-session-operator \
--oidc-client-secret=<client-secret>   # не указывайте, если public client
```

Пользователи аутентифицируются обычным логином и паролем Keycloak — прокси автоматически обменивает их на JWT.

### 6. Подключение через прокси

**DBeaver (Thrift):**
1. Создайте новое подключение Apache Hive
2. Host: `<proxy-endpoint>`, Port: `10009`
3. Аутентификация: введите доменные логин и пароль
4. Сессия создаётся автоматически при подключении

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

Или создайте session CR напрямую (для автоматизации):

```sh
kubectl apply -f config/samples/session.yaml
```

## Конфигурация

Оператор принимает следующие флаги командной строки:

| Флаг | По умолчанию | Описание |
|------|-------------|----------|
| `--namespace` | `spark-dev` | Namespace для отслеживания и управления ресурсами |
| `--gateway-addr` | `:8080` | Адрес REST API gateway |
| `--thrift-proxy-addr` | `:10009` | Адрес Thrift-прокси |
| `--connect-proxy-addr` | `:15002` | Адрес Spark Connect gRPC-прокси |
| `--oidc-issuer-url` | _(пусто)_ | URL OIDC-издателя (например, `https://keycloak.example.com/realms/spark`) |
| `--oidc-audience` | _(пусто)_ | Ожидаемая OIDC audience |
| `--oidc-user-claim` | `sub` | JWT claim, содержащий имя пользователя |
| `--oidc-groups-claim` | `groups` | JWT claim, содержащий группы пользователя |
| `--oidc-skip-validation` | `false` | Пропустить валидацию OIDC-токена (только для разработки) |
| `--oidc-client-id` | _(пусто)_ | OAuth client ID для Keycloak ROPC grant (используется прокси) |
| `--oidc-client-secret` | _(пусто)_ | OAuth client secret для Keycloak ROPC grant (опционально) |
| `--leader-elect` | `false` | Включить leader election для высокой доступности |
| `--health-probe-bind-address` | `:8081` | Адрес health/readiness проб |
| `--metrics-bind-address` | `0` | Адрес эндпоинта метрик (`0` = отключено) |
| `--metrics-secure` | `true` | Отдавать метрики по HTTPS |

## REST API

Все эндпоинты требуют заголовок `Authorization: Bearer <token>`.

| Метод | Путь | Описание |
|-------|------|----------|
| `GET` | `/api/v1/sessions` | Список ваших сессий |
| `GET` | `/api/v1/sessions/{name}` | Детали сессии |
| `DELETE` | `/api/v1/sessions/{name}` | Завершить сессию |

Неаутентифицированные health-эндпоинты:

| Метод | Путь | Описание |
|-------|------|----------|
| `GET` | `/healthz` | Liveness probe |
| `GET` | `/readyz` | Readiness probe |

### Ответ сессии

Возвращается GET и DELETE эндпоинтами.

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

## Справочник CRD

### SparkSessionPool

```yaml
apiVersion: sparkinteractive.io/v1alpha1
kind: SparkSessionPool
metadata:
  name: connect-pool
  namespace: spark-dev
spec:
  type: connect                    # "connect" или "thrift"
  replicas:
    min: 1                         # Минимум запущенных экземпляров
    max: 5                         # Максимум экземпляров
  scaling:
    metrics:
      type: activeSessions         # activeSessions | cpu | memory
      targetPerInstance: 20         # Семантика целевого значения описана ниже
    scaleUpThreshold: "0.8"        # Увеличение при нагрузке > 80% от target (только activeSessions)
    scaleDownThreshold: "0.3"      # Уменьшение при нагрузке < 30% от target
    cooldownSeconds: 300           # Минимум секунд между действиями скейлинга
    drainBeforeScaleDown: true     # Дождаться завершения сессий перед удалением экземпляра
  sparkApplicationTemplate:
    spec: { ... }                  # Спецификация SparkApplication (передаётся Spark Operator)
  sessionPolicy:
    maxSessionsPerUser: 5
    maxTotalSessions: 200
    idleTimeoutMinutes: 720        # 12 часов
    defaultSessionConf:
      spark.executor.memory: "4g"
    quotas:                        # Переопределения для пользователей/групп
      - match:
          users: ["admin"]
        maxSessionsPerUser: 20
```

#### Метрики скейлинга

Поле `spec.scaling.metrics.type` выбирает одну метрику для автоскейлинга. Для каждого пула активен только один тип метрики.

| Тип | Значение `targetPerInstance` | Target по умолчанию | Источник данных |
|-----|------------------------------|---------------------|----------------|
| `activeSessions` | Целевое количество сессий на экземпляр | 20 | SparkInteractiveSession CR |
| `cpu` | Целевой % использования CPU (0–100) | 80 | Kubernetes Metrics API (`metrics.k8s.io`) |
| `memory` | Целевой % использования памяти (0–100) | 80 | Kubernetes Metrics API (`metrics.k8s.io`) |

Для `cpu` и `memory` контроллер считывает потребление ресурсов driver-пода из Metrics API и сравнивает с resource requests. Желаемое количество реплик вычисляется по формуле HPA: `ceil(currentReplicas * avgUtilization / target)`. Для этого в кластере должен быть установлен [Metrics Server](https://github.com/kubernetes-sigs/metrics-server) (или эквивалентный провайдер `metrics.k8s.io`).

Для `activeSessions` контроллер считает активные/неактивные SparkInteractiveSession CR, назначенные каждому экземпляру. `scaleUpThreshold` создаёт запас: если средняя нагрузка на экземпляр превышает `target * scaleUpThreshold`, добавляется дополнительная реплика.

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

## Разработка

### Локальный запуск (вне кластера)

```sh
# Установка CRD
make install

# Запуск оператора с текущим kubeconfig
make run
```

### Сборка

```sh
# Генерация deepcopy и CRD-манифестов
make generate manifests

# Сборка бинарного файла
CGO_ENABLED=0 go build -o bin/manager cmd/main.go
```

### Запуск тестов

```sh
make test
```

### Структура проекта

```
├── api/v1alpha1/                  # Определения типов CRD
│   ├── groupversion_info.go
│   ├── sparksessionpool_types.go
│   ├── sparkinteractivesession_types.go
│   └── zz_generated.deepcopy.go   # Сгенерировано
├── cmd/main.go                    # Точка входа
├── internal/
│   ├── auth/token.go              # Общая OIDC-аутентификация + Keycloak ROPC
│   ├── controller/                # Логика reconciliation
│   │   ├── sparksessionpool_controller.go
│   │   └── sparkinteractivesession_controller.go
│   ├── gateway/server.go          # REST API gateway (чтение + удаление)
│   └── proxy/                     # Прокси с автосозданием сессий
│       ├── proxy.go               # Жизненный цикл сессий + keepalive
│       ├── thrift_sasl.go         # Парсер SASL PLAIN фреймов
│       └── connect_grpc.go        # Извлечение gRPC metadata + raw codec
├── config/
│   ├── crd/bases/                 # Сгенерированные CRD YAML
│   ├── manager/manager.yaml       # Манифест Deployment
│   ├── rbac/                      # Сгенерированный RBAC из маркеров
│   └── samples/                   # Примеры CR
├── Dockerfile
├── Makefile
└── PROJECT                        # Метаданные проекта Kubebuilder
```

## Удаление

```sh
# Удаление CR
kubectl delete -k config/samples/

# Удаление оператора
make undeploy

# Удаление CRD
make uninstall
```

## Лицензия

Copyright 2026 Tander.

Лицензировано под Apache License, Version 2.0.
