# livekit-server relay fork (mesh-relay Phase F)

Fork of [livekit/livekit](https://github.com/livekit/livekit) @ **v1.13.2** carrying the
native inter-server relay from [atdevten/mesh-relay](https://github.com/atdevten/mesh-relay)
(design: that repo's `guide.md` §4 and `docs/approach-b-solution-architecture.md`).

Branch layout: `master` tracks upstream untouched; **`relay`** is this fork's line
(v1.13.2 + the relay package). Weekly upstream merges land as PRs via
`.github/workflows/upstream-sync.yml`.

## What this fork adds

**One new package — `pkg/relay/` — and nothing else yet.** New files never conflict on
upstream merges (mesh-relay pain point B12); the wiring hooks below are the only future
edits to upstream files, kept to a few lines each.

| File | What it is |
|---|---|
| `pkg/relay/downtrack.go` | `RelayDownTrack` — implements `sfu.TrackSender`, registered on a receiver's fan-out like any subscriber; simultaneously implements mesh-relay's `SourceTrack` + `SenderReportSource`. Exports a local track to peers. |
| `pkg/relay/receiver.go` | `SyntheticReceiver` — embeds `sfu.ReceiverBase` (the engine behind `WebRTCReceiver`), built from a relayed `TrackUpdate`'s codec params; per-layer `buffer.Buffer`s are fed by the relay instead of a WebRTC up-track. Implements mesh-relay's `SinkTrack` + `SenderReportSink`. Local downtracks attach normally. |
| `pkg/relay/agent.go` | Per-node runtime: owns the QUIC link + engines; `ExportTrack`/`UnexportTrack` (origin side) and `OnRelayedTrack` (edge side) are the hooks the room layer calls. |
| `pkg/relay/logger.go` | logr→zap bridge for the mesh-relay engines. |

The protocol itself (QUIC + FlatBuffers, per-hop NACK, subscription gating + hysteresis,
SR forwarding, metrics) is the imported `github.com/atdevten/mesh-relay/relay` module —
reused unchanged, tested there.

## How the pieces map (mesh-relay §4.4: "a relay peer is just another participant")

```text
local publisher ─► WebRTCReceiver ─fan-out─► DownTracks (local viewers)
                                  └────────► RelayDownTrack ─► OriginEngine ═QUIC═►
═QUIC═► EdgeEngine ─► SyntheticReceiver ─fan-out─► DownTracks (local viewers)
```

- Cross-relay keyframes: viewer PLI → `SyntheticReceiver.onRTCPFeedback` →
  `KeyFrameRequest` upstream → `RelayDownTrack.RequestKeyFrame` → `receiver.SendPLI`
  toward the real publisher.
- Lip-sync (B4): publisher SRs → `HandleRTCPSenderReportData` → relay `SenderReport` →
  `buffer.SetSenderReportData` on the synthetic side → `ReceiverBase` propagates to
  every local subscriber.
- Simulcast: `RelayDownTrack` sees every layer the receiver holds (native tap — the
  SDK harness's 3-connection hack does not exist here). Per-layer demand flows back as
  Subscribe bitmasks into `EdgeEngine.SetLayerDemand`.

## Status / not wired yet

`pkg/relay` compiles against v1.13.2 and the full server builds. **The room-layer wiring
is not done** — the hooks exist but nothing calls them yet:

1. Track-published path → `agent.ExportTrack(receiver)`; unpublish → `UnexportTrack`.
2. `agent.OnRelayedTrack` → create/lookup the remote participant (identity =
   `TrackInfo.SessionID`) and announce the `SyntheticReceiver` as its published track.
3. Downtrack subscribe/unsubscribe on synthetic receivers → `EdgeEngine.SetLayerDemand`.
4. Node config (peer addr / listen addr) + agent lifecycle in server startup.
5. Behavior validation: `StreamTrackerManagerConfig` zero-value, buffer sizing, and the
   `livekit.TrackInfo` fields the room layer expects on a synthetic track.

## Building

The mesh-relay module is private:

```bash
export GOPRIVATE=github.com/atdevten
gh auth setup-git       # or a PAT in ~/.netrc
go build ./...
```

CI (upstream-sync workflow) needs a `MESH_RELAY_TOKEN` repo secret with read access to
atdevten/mesh-relay.
