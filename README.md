# spandrel

A distributed tracing backend: it accepts OpenTelemetry spans, reassembles them
into traces, and decides which traces are worth keeping after it has seen the
whole thing rather than at the moment the request started.

Early days. The span model and the wire protocol land first; the sampler, which
is the part I actually care about, comes after there is something to sample.
