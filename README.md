# ollama-priority-proxy

[![Release](https://github.com/USA-RedDragon/ollama-priority-proxy/actions/workflows/release.yaml/badge.svg)](https://github.com/USA-RedDragon/ollama-priority-proxy/actions/workflows/release.yaml) [![coverage](https://raw.githubusercontent.com/USA-RedDragon/ollama-priority-proxy/main/.github/badges/coverage.svg)](https://github.com/USA-RedDragon/ollama-priority-proxy/actions) [![go.mod version](https://img.shields.io/github/go-mod/go-version/USA-RedDragon/ollama-priority-proxy.svg)](https://github.com/USA-RedDragon/ollama-priority-proxy) [![License](https://badgen.net/github/license/USA-RedDragon/ollama-priority-proxy)](https://github.com/USA-RedDragon/ollama-priority-proxy/blob/main/LICENSE) [![Release](https://img.shields.io/github/release/USA-RedDragon/ollama-priority-proxy.svg)](https://github.com/USA-RedDragon/ollama-priority-proxy/releases/)

A reverse proxy for one Ollama server that is shared by people and by background work. People's requests always go first. Background requests go only when no people's request is running or waiting.

## Usage

Point clients at the proxy instead of Ollama. The proxy forwards the whole Ollama HTTP API, both the OpenAI-compatible `/v1/...` and the native `/api/...` paths, with the method, path, query, headers and body unchanged. Streaming responses (SSE and NDJSON) are sent on chunk by chunk.

- People's clients use the proxy as is, for example `http://proxy:11434/v1`. Their requests are high priority.
- Background clients put `/low` in front of the path, for example `http://proxy:11434/low/v1`. Their requests are low priority. The proxy removes `/low` before it forwards the request. A request with the header `X-Priority: low` is also low priority; the header is removed before forwarding.

### Scheduling

Only POST requests to the paths in `generation-paths` are scheduled. All other requests, such as `/api/tags`, `/api/show`, `/api/ps`, `/api/version` and `/v1/models`, go to Ollama at once in both classes.

- A high priority request goes to Ollama at once, with no limit. Ollama queues them itself.
- A low priority request waits in a first-in, first-out queue. It goes to Ollama only when no high priority request is in flight, `low.idle-grace` has passed since the last one finished, and fewer than `low.max-in-flight` low priority requests are in flight.
- A high priority request that arrives while low priority requests wait goes first. The waiting requests stay in the queue.
- A low priority request that is already in flight is not stopped.
- When a client disconnects, the proxy cancels its upstream request, so Ollama stops generating. A queued request leaves the queue.
- With `low.max-wait` set, a low priority request that waits longer fails with `503` and a `Retry-After` header.

The idle grace keeps the steps of an agent's turn together: a tool call and the next model call often have a short gap between them, and without the grace a background request could start in that gap and make the next step wait for it. A longer grace protects longer gaps but delays background work after every turn. Set it to `0` to send background work as soon as Ollama is free.

A request is in flight until its response body has been fully sent. A low priority request body is read into memory before the request is queued, so that the proxy notices when the client disconnects. Bodies over 64 MiB fail with `413`.

### Health

- `/health` answers `200 OK` without contacting Ollama. Use it for liveness probes.
- `/ready` answers `200 OK` when `GET /api/version` on Ollama succeeds within 3 seconds, and `503` otherwise.

These two paths are served by the proxy and never forwarded.

### Metrics

When `metrics.enabled` is true, Prometheus metrics are on `/metrics` of the metrics listener, separate from the proxy port. `route` is the generation path, or `other` for requests that are not scheduled. `class` is `high` or `low`.

| Metric | Type | Labels | Description |
|---|---|---|---|
| `ollama_priority_proxy_requests_total` | counter | `class`, `route`, `code` | Requests handled. `code` 499 means the client disconnected |
| `ollama_priority_proxy_errors_total` | counter | `class`, `route`, `reason` | Failed requests. `reason` is `upstream`, `max_wait`, `canceled` or `bad_request` |
| `ollama_priority_proxy_in_flight` | gauge | `class` | Generation requests sent to Ollama and not yet finished |
| `ollama_priority_proxy_queue_depth` | gauge | | Low priority requests waiting |
| `ollama_priority_proxy_queue_wait_seconds` | histogram | `class` | Time generation requests waited before they were sent |
| `ollama_priority_proxy_upstream_duration_seconds` | histogram | `class`, `route` | Time from sending a request to Ollama until its response was fully copied |

Logs have one line per request with the method, path, class, route, status, wait and duration. They never contain request or response bodies.

## Configuration

Configuration is read from `config.yaml` in the working directory, or from the file given with `--config`/`-c`. Environment variables and flags override the file. A full example is in [`config.example.yaml`](config.example.yaml).

<!-- configulator:begin -->

| Key                 | Type           | Default                                                                                                                | Environment         | Flag                  | Description                                                                                                                                                                                                              |
|---------------------|----------------|------------------------------------------------------------------------------------------------------------------------|---------------------|-----------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `log-level`         | string         | `info`                                                                                                                 | `LOG_LEVEL`         | `--log-level`         | Logging level. One of debug, info, warn or error                                                                                                                                                                         |
| `http.bind`         | string         | `[::]`                                                                                                                 | `HTTP_BIND`         | `--http.bind`         | Address to listen on. The default, [::], listens on all interfaces                                                                                                                                                       |
| `http.port`         | integer        | `11434`                                                                                                                | `HTTP_PORT`         | `--http.port`         | Port to serve the proxy, /health and /ready on                                                                                                                                                                           |
| `metrics.enabled`   | boolean        | `true`                                                                                                                 | `METRICS_ENABLED`   | `--metrics.enabled`   | Serve Prometheus metrics on /metrics                                                                                                                                                                                     |
| `metrics.bind`      | string         | `[::]`                                                                                                                 | `METRICS_BIND`      | `--metrics.bind`      | Address for the metrics listener                                                                                                                                                                                         |
| `metrics.port`      | integer        | `9090`                                                                                                                 | `METRICS_PORT`      | `--metrics.port`      | Port for the metrics listener                                                                                                                                                                                            |
| `upstream.url`      | string         | `http://127.0.0.1:11434`                                                                                               | `UPSTREAM_URL`      | `--upstream.url`      | Base URL of the Ollama server                                                                                                                                                                                            |
| `low.max-in-flight` | integer        | `1`                                                                                                                    | `LOW_MAX_IN_FLIGHT` | `--low.max-in-flight` | Most low priority generation requests sent to the upstream at once                                                                                                                                                       |
| `low.idle-grace`    | string         | `2s`                                                                                                                   | `LOW_IDLE_GRACE`    | `--low.idle-grace`    | How long no high priority generation request must be in flight before a low priority one is sent, so the steps of an agent's turn are not split by background work. Longer values delay background work more; 0 disables |
| `low.max-wait`      | string         | `0s`                                                                                                                   | `LOW_MAX_WAIT`      | `--low.max-wait`      | How long a low priority request may wait in the queue before it fails with 503 and Retry-After; 0 waits forever                                                                                                          |
| `generation-paths`  | list of string | `/v1/chat/completions,/v1/completions,/v1/embeddings,/v1/responses,/api/chat,/api/generate,/api/embed,/api/embeddings` | `GENERATION_PATHS`  | `--generation-paths`  | Upstream paths whose POST requests are scheduled by priority. Other requests go straight to the upstream                                                                                                                 |

<!-- configulator:end -->
