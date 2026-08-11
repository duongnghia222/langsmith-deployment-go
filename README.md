# LSD — LangSmith Deployment

An independent Go implementation of the LangSmith Deployment `core-api` backend — assistants, threads, runs, crons, checkpointer, and cache. It speaks the same protobuf API as upstream `core-api` (protos are vendored from the Python `langgraph_api` package, see "Protos" below), so an existing `langgraph_api` deployment can point at it unchanged via `LSD_GRPC_SERVER_ADDRESS`.

The `LSD_*` variables below configure *this* server only; they are a separate set from the `LSD_*` variables the Python side reads.

> Not affiliated with or endorsed by LangChain. This is a clean-room reimplementation of a wire protocol for self-hosting.

Backed by Postgres (with `pgvector`) and Redis (cache + streams).

## Services

Registered on the gRPC server (`:50051` by default):

| Service       | Proto package    | Notes                                                          |
|---------------|------------------|----------------------------------------------------------------|
| `Admin`       | `lsd.v1`, `coreApi` | `Capabilities` advertises version, schema, services, features. |
| `Assistants`  | `coreApi`        | Assistant + versioned config CRUD.                             |
| `Threads`     | `coreApi`        | Thread CRUD, search, TTL, values filter.                       |
| `Runs`        | `coreApi`        | Run lifecycle, lease/heartbeat, `after_seconds`, stats.        |
| `Crons`       | `coreApi`        | Cron CRUD, `thread_filters`, `Next.now`.                       |
| `Checkpointer`| `checkpointer`   | Checkpoint reads/writes with task-path support.                |
| `Cache`       | `coreApi`        | Redis-backed cache RPCs.                                       |
| `grpc.health.v1.Health` | -      | Always-serving health probe.                                   |
| Reflection    | -                | Enabled for `grpcurl` and similar tools.                       |

Two background workers run inside the process:

- `runs.RunReaper` — reclaims runs whose lease expired.
- `crons.CronScheduler` — fires due crons and creates the corresponding runs.

## Quick start

Requires Docker. For running on the host instead of in a container, also Go 1.26+ (and `buf` if you plan to regenerate protos).

### Everything in Docker (one command)

```sh
docker compose up -d --build
```

This builds the `lsd` image and brings up Postgres, Redis, and the LSD server. Compose `depends_on` waits for the data stores to be healthy before LSD starts. Migrations run automatically on startup.

Smoke-test it:

```sh
grpcurl -plaintext localhost:50051 list
grpcurl -plaintext localhost:50051 lsd.v1.Admin/Capabilities
```

Tail logs / stop:

```sh
docker compose logs -f lsd
docker compose down          # add -v to wipe the Postgres volume
```

### Run on the host (faster dev loop)

Useful when you're iterating on Go code — `go build` is ~1s vs. a full image rebuild.

```sh
# 1. Start just the data stores
docker compose up -d postgres redis

# 2. Load env (.env.example points at localhost)
set -a; source .env.example; set +a

# 3. Build and run
make build
./bin/lsd
```

## Configuration

All config is read from environment variables at startup (see `internal/config/config.go`).

| Variable | Required | Default | Description |
|---|---|---|---|
| `LSD_DATABASE_URL` | yes | — | Postgres DSN (e.g. `postgres://lsd:lsd@localhost:5432/lsd?sslmode=disable`) |
| `LSD_REDIS_URL` | yes | — | Redis URL (e.g. `redis://localhost:6379/0`) |
| `LSD_DB_POOL_MAX_CONNS` | no | `50` | Postgres pool max connections (Python reference runs 150) |
| `LSD_REDIS_POOL_SIZE` | no | `100` | Redis client pool size (blocking stream reads each hold a connection) |
| `LSD_GRPC_ADDR` | no | `:50051` | gRPC listen address |
| `LSD_METRICS_ADDR` | no | `:9090` | Prometheus metrics listen address |
| `LSD_LEASE_TTL_SECONDS` | no | `30` | Run lease TTL |
| `LSD_HEARTBEAT_INTERVAL_SECONDS` | no | `5` | Expected client heartbeat cadence |
| `LSD_REAPER_INTERVAL_SECONDS` | no | `2` | How often the run reaper sweeps |
| `LSD_NEXT_POLL_INTERVAL_SECONDS` | no | `1` | `Runs.Next` long-poll interval |
| `LSD_STREAM_MAX_LEN` | no | `1000` | Redis stream `MAXLEN ~` for run events |
| `LSD_STREAM_READ_BLOCK_MS` | no | `5000` | `XREAD` block duration |
| `LSD_STREAM_REPLAY_BATCH` | no | `100` | Stream replay batch size |
| `LSD_LOG_LEVEL` | no | `info` | `debug` / `info` / `warn` / `error` |
| `LSD_ENV` | no | `prod` | Environment tag |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | no | unset | If unset, tracing is a no-op. |

## Repository layout

```
cmd/lsd/             # main entrypoint, wires deps and runs background loops
internal/
  admin/             # Admin / Capabilities service
  assistants/        # Assistants service + store
  threads/           # Threads service + store
  runs/              # Runs service, store, reaper
  crons/             # Crons service, store, scheduler
  checkpointer/      # Checkpointer service + store
  cache/             # Cache service over Redis
  stream/            # Redis Streams publisher/replay
  server/            # gRPC server wiring (registration, health, reflection)
  config/            # env-based config loader
  db/                # pgx pool, embedded migrations
  redis/             # Redis client wrapper
  auth/, logger/,    # cross-cutting
  metrics/, tracing/,
  jsonbutil/, testdb/
proto/               # vendored .proto sources (see "Protos" below)
gen/                 # generated Go code from buf
scripts/dump_proto.py  # vendors protos from the Python source
test/integration/    # gRPC-level integration tests
test/streaming/      # soak tests for streaming
```

## Development

```sh
make build      # build ./bin/lsd
make test       # go test ./...
make fmt vet    # formatting + vet
make tidy       # go mod tidy
make clean      # rm -rf bin
```

Integration tests under `test/integration/` and `test/streaming/` use `testcontainers-go` to spin up Postgres and Redis, so Docker must be running.

### Protos

The `.proto` files under `proto/` are vendored from the upstream Python project — they are checked in, so nothing below is needed for a normal build. To refresh them against a Python `langgraph_api` source tree and regenerate the Go bindings:

```sh
export LANGGRAPH_API_ROOT=/path/to/api   # dir containing langgraph_grpc_common/
make proto-bootstrap PYTHON=python3      # re-dump protos from Python source
make gen                                 # regenerate gen/ via buf
```

To check the vendored protos have not drifted from your Python source:

```sh
make proto-check
```

### Migrations

SQL migrations live in `internal/db/migrations/` and are embedded into the binary via `go:embed`. `db.Migrate` runs them on startup; there is no separate migrate CLI to invoke. To add one, drop a new pair of `NNNNNNN_<name>.up.sql` / `.down.sql` files following the existing numbering.

## Observability

- **Metrics** — Prometheus endpoint on `LSD_METRICS_ADDR` (`/metrics`).
- **Tracing** — OpenTelemetry over OTLP/gRPC when `OTEL_EXPORTER_OTLP_ENDPOINT` is set; otherwise the global tracer provider is a no-op and spans are discarded cheaply.
- **Health** — `grpc.health.v1.Health/Check` returns `SERVING` once the server is up.
