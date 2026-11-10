# LevelDB backend (experimental)

This branch adds a LevelDB-backed implementation of the existing GoMQTT broker
Backend interface. The original package layout and MemoryBackend are retained.

## Opening the backend

```go
backend, err := broker.OpenLevelDBBackend("/var/lib/my-broker/leveldb",
    broker.LevelDBBackendOptions{
        SessionQueueSize: 1000,
        KillTimeout:     5 * time.Second,
        Credentials: map[string]string{
            "agent": "replace-this-password",
        },
    },
)
if err != nil {
    return err
}

engine := broker.NewEngine(backend)
port, quit, done := broker.Run(engine, "tcp")
_ = port // publish the selected port through your application configuration

// During shutdown: stop accepting connections, wait for the server, then close
// the backend so active clients terminate before the LevelDB handle is closed.
close(quit)
<-done
if !backend.Close(10 * time.Second) {
    // The timeout elapsed. Keep the process alive and retry Close after clients
    // have finished; do not remove or copy the database directory while open.
}
```

Import `time` and `github.com/256dpi/gomqtt/broker` in the application.

## Persistence model

- Persistent client IDs (`CleanSession=false`) have a durable packet session,
  subscription list, and offline QoS 1/2 delivery queue.
- Retained messages are stored by topic and survive a broker restart.
- `CleanSession=true` removes the previous durable session, subscriptions,
  queued deliveries, and inflight packet records before creating a temporary
  session.
- QoS 0 messages are not written to an offline persistent queue. They may be
  buffered in memory while that session is online.
- A queued QoS 1/2 delivery is removed by the Backend Ack callback only after
  the GoMQTT Client has saved the outgoing packet in its Session. The MQTT
  protocol exchange then continues using the persisted inflight packet.
- The database is opened and owned by `LevelDBBackend`; do not open the same
  LevelDB directory from another process or close its internal handle yourself.

## Durability and delivery semantics

All backend writes that precede MQTT acknowledgements use LevelDB synchronous
writes. QoS 1 delivery remains at-least-once: if a process fails after the
outgoing packet was persisted but before the remote client completes the
protocol exchange, a retransmission can occur. Applications should make
business operations idempotent using their own job/message identifiers.

The legacy GoMQTT `Session.NextID()` signature cannot return an error. The
LevelDB implementation records an error encountered while advancing the
persistent packet ID counter; callers can inspect it with
`LevelDBSession.LastError()`, and subsequent packet operations also return the
recorded error.

## Current scope and limitations

This is an initial backend implementation, not a replacement for a full
production broker review. In particular, persistent offline queue quotas,
message expiry policies, crash-injection tests, long-running load tests,
multi-process access, and a full MQTT QoS 2 conformance matrix still require
additional work. The Backend Ack callback has no error return in the upstream
interface; deletion failures are reported through the configured Logger.

## Tests

The branch CI runs the broker and session package tests on Go 1.20 and Go 1.23,
including a pure-Go build with CGO disabled, race detection on Go 1.23 and
`go vet`. A separate workflow runs the
repository's complete test suite with Mosquitto and generated TLS test
certificates.
