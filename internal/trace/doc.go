// Package trace holds the domain model every other package is written
// against: identifiers, spans, and the typed attribute values hanging off
// them.
//
// It is deliberately free of wire-format concerns. The OpenTelemetry protocol
// is one way spans arrive and the storage layer is one way they are kept, but
// neither gets to decide what a span is, so nothing in here imports either.
package trace
