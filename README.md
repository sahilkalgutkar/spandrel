# spandrel

[![CI](https://github.com/sahilkalgutkar/spandrel/actions/workflows/ci.yml/badge.svg)](https://github.com/sahilkalgutkar/spandrel/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/sahilkalgutkar/spandrel/branch/main/graph/badge.svg)](https://codecov.io/gh/sahilkalgutkar/spandrel)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A distributed tracing backend: it accepts OpenTelemetry spans, reassembles them
into traces, and decides which traces are worth keeping after it has seen the
whole thing rather than at the moment the request started.

What works so far: ingest over OTLP gRPC and HTTP, storage in memory or on
disk, and tail sampling in between. I hold each trace until it goes quiet,
then keep it if it has an error, if it was slow, or as a fixed share of the
rest, with a cap per service so one service in trouble can't take the whole
budget. A span that arrives after its trace was judged follows the verdict
instead of being judged on its own. Querying traces back out comes next, then
a proper write-up of the sampling design.

## Running it

```bash
go run ./cmd/spandreld -data-dir ./data
```

Point any OpenTelemetry SDK at it. It listens on 4317 for gRPC and 4318 for
HTTP, the ports every SDK uses by default. Without `-data-dir` it keeps spans
in memory.

The sampling flags, all optional:

| Flag | Default | What it does |
|---|---|---|
| `-sample-quiet` | `5s` | how long a trace goes without a new span before it is judged |
| `-sample-max-age` | `1m` | how long a trace can wait to be judged however busy it is |
| `-sample-buffer` | `200000` | most spans held at once; past it, exports get a retryable error |
| `-keep-slower-than` | `1s` | keep every trace that took longer; `0` turns it off |
| `-keep-share` | `0.1` | share of the remaining traces to keep |
| `-service-rate` | `50` | most traces a second kept for one service; `0` means no cap |
| `-service-burst` | `100` | traces a service can have kept back to back before the rate applies |
| `-remember` | `5m` | how long a verdict is remembered so late spans follow it |
| `-max-decisions` | `100000` | most verdicts remembered at once |

Error traces are always kept, subject only to the per-service cap. To keep
everything, use `-keep-share 1 -service-rate 0`.

## Building

Go 1.25 or newer.

```bash
make test
```
