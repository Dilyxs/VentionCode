# Submitted as a code sample for Vention

A self-contained Go package extracted from a real-time multiplayer game project I've been working on. It demonstrates the actor pattern with command/reply channels, broadcast with non-blocking send for slow consumers, and context-based cancellation — alongside a test suite covering happy paths, edge cases, and concurrency-shape behaviors under the race detector.

This is a stripped-down version of [`realtime1vs1/backend/lib`](https://github.com/Dilyxs/realtime1vs1/tree/main/backend/lib). The original package depends on a websocket hub, a JSON message protocol, and several application-specific types. Here those concerns are removed: domain types are reduced to primitives or small local structs, websocket I/O is replaced with plain `chan Message` channels, and the broadcast destination is a generic listener registry rather than a websocket connection map. The actor patterns and concurrency semantics are unchanged.

## What's in here

- **`Room`** — an actor that owns membership state (`AllowedPlayers`), a registry of message listeners (`SocketsConn`), and a log of internal events (`PreviousMessages`). It accepts external commands (`AddPlayerCommand`, `CheckIfUserAllowedToJoin`, `AddPlayerToWebsocketCommand`) over one channel and internal events (`UserDisconnection` and broadcast messages) over another.
- **`Manager`** — an actor that drives a question/answer loop, evaluates answers from players, and emits aggregated results.
- **Tests** for both, structured around a fixture pattern, generic `assertReceives` / `assertNoReceive` helpers, and an order-insensitive slice comparison helper for set-equality assertions.

## Running

To run the tests, simply execute:

```bash
go test -v -race
```
