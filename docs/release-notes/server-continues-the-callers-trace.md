---
type: feature
---

- `lore-server`: A gRPC request carrying W3C trace context (`traceparent`, `tracestate`) now continues the caller's
  trace, and follows the caller's sampling decision, instead of always starting a new trace
