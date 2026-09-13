---
"mixed-signals": patch
---

Refresh model references whose client facades have been garbage-collected before delivering RPC results, notifications, or peer calls.
Preserve message order during recovery and propagate refresh failures without exposing incomplete models.
