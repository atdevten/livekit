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
| `pkg/relay/service.go` | Link supervisor (dial-with-backoff / accept loops) started with the server, plus the nil-safe `HandleTrackPublished`/`HandleTrackUnpublished` hooks the room calls. |
| `pkg/relay/logger.go` | logr→zap bridge for the mesh-relay engines. |
| `pkg/config/relay.go` | `RelayConfig` (`relay:` yaml block — `enabled`, `listen_address`, `peer_address`). |

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

`pkg/relay` compiles against v1.13.2 and the full server builds. The adapters
(`RelayDownTrack`, `SyntheticReceiver`, `Agent`) are complete and satisfy both the SFU
interfaces and the mesh-relay seam (compile-time asserted). **Origin-side room wiring is
done; edge-side wiring is blocked** — see below.

### Origin side (done)

Enable with the `relay:` config block; the server starts a `relay.Service` alongside
`signalServer` and stops it on shutdown:

```yaml
relay:
  enabled: true
  peer_address: 10.0.0.2:7810   # origin side: dial this edge
  listen_address: 0.0.0.0:7810  # edge side: accept origins (may set both)
```

Upstream edits, kept to a line or two per site (B12):

1. `pkg/rtc/room.go` `onTrackPublished` → `relay.HandleTrackPublished(participant, track)`;
   it waits for receiver readiness via `AddOnReady` (a pre-media `DummyReceiver` has no
   layer/codec info yet), then `agent.ExportTrack(receivers[0])`.
2. `onTrackUnpublished` → `relay.HandleTrackUnpublished(track)`.
3. `pkg/service/server.go` constructs/starts/stops the service; `pkg/config/config.go`
   gains the one `Relay RelayConfig` field.

Notes: exports never block the publish path (announcements queue in the Agent and drain
into the link's source loop), every exported track is re-announced after a reconnect, and
tracks published under the `__relay__` identity prefix are skipped — the cycle guard until
Phase 4 topology lands.

### Edge side (BLOCKED on the participant model — this is the F↔Phase-5 boundary)

`agent.OnRelayedTrack` must present the `SyntheticReceiver` as a **published track owned by a
participant** so local clients discover and subscribe to it. Investigation of v1.13.2:

- `Room.participants` is `map[identity]types.LocalParticipant`; a track is only subscribable
  through a participant in that map.
- `types.LocalParticipant` is a **134-method interface**, and its sole implementation
  `rtc.ParticipantImpl` is transport-bound: `ParticipantParams` requires a `routing.MessageSink`,
  signal transport, and PeerConnections. There is **no transport-less publisher** in OSS.
- `IsRelayed` exists as a field threaded through the subscription path but is **never set true**
  anywhere in OSS — it is a vestige of LiveKit Cloud's closed relay, with no constructor or
  lifecycle behind it.

So presenting a relayed track requires a publisher participant **with no WebRTC transport** —
which is precisely the `Participant` (metadata) vs `LocalParticipant` (metadata + transport)
split that mesh-relay's `guide.md` §4.1 and architecture §5.1 assign to the **message-bus
control plane (Phase 5)**. Phase F's edge wiring is therefore entangled with Phase 5; it cannot
be completed as a standalone "hook" against stock livekit.

**Options (deferred to a decision):**

- **v1 (single relay publisher):** one synthetic `__relay__` participant owns all relayed
  tracks (accept temporary identity-ghosting, the documented Approach-A limit). Still requires
  a transport-less participant, but only one, and unblocks live validation of the real wins
  (~0 ms path, native simulcast, real decode). Smallest path to a runnable node pair.
- **v2 (per-origin participants):** the full §4.1 split driven by bus state → true identity.
  This is Phase 5 proper.

Either way the missing primitive is a **transport-less participant**, which is Phase-5 work.
Recommend building v1's minimal synthetic participant as the first Phase-5 deliverable, then
completing edge wiring on top of it.

### Not yet validated

Nothing here has run. Every artifact is compile-only. "Room wiring done" means two fork nodes,
publish to A, a viewer decodes the relayed track on B — not a green build. Buffer sizing,
`StreamTrackerManagerConfig` zero-value tolerance, and the exact `livekit.TrackInfo` fields a
synthetic track needs are all unverified until that runs.

## Building

The mesh-relay module is private:

```bash
export GOPRIVATE=github.com/atdevten
gh auth setup-git       # or a PAT in ~/.netrc
go build ./...
```

CI (upstream-sync workflow) needs a `MESH_RELAY_TOKEN` repo secret with read access to
atdevten/mesh-relay.
