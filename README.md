# langsmith-deployment-go

A Go implementation of the LangSmith Deployment `core-api` backend, covering assistants, threads, runs, crons, checkpoints, and cache. It speaks the same protobuf API as the upstream Python `core-api`, so an existing `langgraph_api` deployment can point at it unchanged by setting `LSD_GRPC_SERVER_ADDRESS`.

Built on PostgreSQL (with `pgvector`) and Redis.

> **Note:** This project is not affiliated with or endorsed by LangChain. It is an independent, clean-room reimplementation of the wire protocol for self-hosting purposes.

## Features

- **Assistants** — assistant CRUD with versioned configs
- **Threads** — thread CRUD, search, TTL, and values filtering
- **Runs** — full run lifecycle with leasing, heartbeats, delayed runs (`after_seconds`), and stats
- **Crons** — cron CRUD with `thread_filters`; a built-in scheduler fires due crons and creates runs
- **Checkpointer** — checkpoint reads/writes with task-path support
- **Cache** — Redis-backed cache RPCs
- **Streaming** — run events published over Redis Streams with replay support
- **Operations** — Prometheus metrics, OpenTelemetry tracing, gRPC health checks, and server reflection

Two background workers run inside the server process: a run reaper that reclaims runs with expired leases, and the cron scheduler.

## Requirements

- Docker (for the data stores, or to run everything in containers)
- Go 1.26+ (only if building/running on the host)
- `buf` (only if regenerating protobuf code)

## Getting started

### Run everything with Docker Compose

```sh
docker compose up -d --build
```

This builds the server image and starts PostgreSQL, Redis, and the server. Database migrations run automatically on startup.

Verify it's up:

```sh
grpcurl -plaintext localhost:50051 list
grpcurl -plaintext localhost:50051 lsd.v1.Admin/Capabilities
```

Logs and teardown:

```sh
docker compose logs -f lsd
docker compose down          # add -v to also remove the Postgres volume
```

### Run on the host

Faster for local development — only the data stores run in Docker:

```sh
# 1. Start Postgres and Redis
docker compose up -d postgres redis

# 2. Load environment variables (.env.example points at localhost)
set -a; source .env.example; set +a

# 3. Build and run
make build
./bin/lsd
```

## Configuration

All configuration is read from environment variables at startup (see `internal/config/config.go`). These `LSD_*` variables configure this server only; they are separate from the `LSD_*` variables the Python client side reads.

| Variable | Required | Default | Description |
|---|---|---|---|
| `LSD_DATABASE_URL` | yes | — | Postgres DSN, e.g. `postgres://lsd:lsd@localhost:5432/lsd?sslmode=disable` |
| `LSD_REDIS_URL` | yes | — | Redis URL, e.g. `redis://localhost:6379/0` |
| `LSD_GRPC_ADDR` | no | `:50051` | gRPC listen address |
| `LSD_METRICS_ADDR` | no | `:9090` | Prometheus metrics listen address |
| `LSD_DB_POOL_MAX_CONNS` | no | `50` | Postgres pool max connections |
| `LSD_REDIS_POOL_SIZE` | no | `100` | Redis client pool size |
| `LSD_LEASE_TTL_SECONDS` | no | `30` | Run lease TTL |
| `LSD_HEARTBEAT_INTERVAL_SECONDS` | no | `5` | Expected client heartbeat cadence |
| `LSD_REAPER_INTERVAL_SECONDS` | no | `2` | How often the run reaper sweeps |
| `LSD_NEXT_POLL_INTERVAL_SECONDS` | no | `1` | `Runs.Next` long-poll interval |
| `LSD_STREAM_MAX_LEN` | no | `1000` | Redis stream `MAXLEN ~` for run events |
| `LSD_STREAM_READ_BLOCK_MS` | no | `5000` | `XREAD` block duration |
| `LSD_STREAM_REPLAY_BATCH` | no | `100` | Stream replay batch size |
| `LSD_LOG_LEVEL` | no | `info` | `debug`, `info`, `warn`, or `error` |
| `LSD_ENV` | no | `prod` | Environment tag |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | no | unset | OTLP endpoint for tracing; if unset, tracing is a no-op |

## gRPC services

The server (default `:50051`) registers the following services:

| Service | Proto package | Description |
|---|---|---|
| `Admin` | `lsd.v1`, `coreApi` | Version, schema, services, and feature capabilities |
| `Assistants` | `coreApi` | Assistant and versioned config CRUD |
| `Threads` | `coreApi` | Thread CRUD, search, TTL, values filter |
| `Runs` | `coreApi` | Run lifecycle, lease/heartbeat, `after_seconds`, stats |
| `Crons` | `coreApi` | Cron CRUD, `thread_filters`, `Next.now` |
| `Checkpointer` | `checkpointer` | Checkpoint reads/writes with task-path support |
| `Cache` | `coreApi` | Redis-backed cache RPCs |
| `grpc.health.v1.Health` | — | Health probe; returns `SERVING` once up |

Server reflection is enabled, so tools like `grpcurl` work out of the box.

## Project structure

```
cmd/lsd/               # main entrypoint
internal/
  admin/               # Admin / Capabilities service
  assistants/          # Assistants service + store
  threads/             # Threads service + store
  runs/                # Runs service, store, reaper
  crons/               # Crons service, store, scheduler
  checkpointer/        # Checkpointer service + store
  cache/               # Cache service over Redis
  stream/              # Redis Streams publisher/replay
  server/              # gRPC server wiring
  config/              # env-based config loader
  db/                  # pgx pool, embedded migrations
  redis/               # Redis client wrapper
proto/                 # vendored .proto sources
gen/                   # generated Go code (buf)
test/integration/      # gRPC-level integration tests
test/streaming/        # streaming soak tests
```

## Development

```sh
make build      # build ./bin/lsd
make test       # go test ./...
make fmt vet    # formatting + vet
make tidy       # go mod tidy
make clean      # remove bin/
```

Integration tests under `test/integration/` and `test/streaming/` use [testcontainers-go](https://github.com/testcontainers/testcontainers-go) to spin up Postgres and Redis, so Docker must be running.

### Migrations

SQL migrations live in `internal/db/migrations/` and are embedded into the binary. They run automatically on startup; there is no separate migrate CLI. To add one, create a new `NNNNNNN_<name>.up.sql` / `.down.sql` pair following the existing numbering.

### Regenerating protos

The `.proto` files under `proto/` are vendored from the upstream Python project and checked in, so this is not needed for a normal build. To refresh them from a Python `langgraph_api` source tree:

```sh
export LANGGRAPH_API_ROOT=/path/to/api   # dir containing langgraph_grpc_common/
make proto-bootstrap PYTHON=python3      # re-dump protos from Python source
make gen                                 # regenerate gen/ via buf
make proto-check                         # verify vendored protos haven't drifted
```

## Observability

- **Metrics** — Prometheus endpoint at `http://<LSD_METRICS_ADDR>/metrics`
- **Tracing** — OpenTelemetry over OTLP/gRPC when `OTEL_EXPORTER_OTLP_ENDPOINT` is set
- **Health** — standard gRPC health checking via `grpc.health.v1.Health/Check`

## License

[MIT](LICENSE)
