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
| `pkg/relayrtc/participant.go` | **`Participant` — the transport-less publisher (Phase 5a)**: full `types.LocalParticipant`, no PeerConnection; transport/signal/migration methods are no-ops, media enters via SyntheticReceivers. Separate package because `pkg/rtc` already imports `pkg/relay` (origin hooks) and this side must import `pkg/rtc`. |
| `pkg/relayrtc/mediatrack.go` | `MediaTrack` — wraps upstream `rtc.MediaTrackReceiver` with `IsRelayed: true` (first real use of the vestige flag) around a `SyntheticReceiver`; downtrack fan-out and subscription bookkeeping are untouched upstream code. |

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
done; edge-side wiring is done** (Phase 5a + announcer) — live two-node validation is
the remaining exit bar.

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

### Phase 5a — transport-less participant (LANDED; unblocks edge wiring)

`pkg/relayrtc` provides the missing primitive. Feasibility findings that shaped it
(step-0 pass, 2026-07-04/05):

- **No concrete `*ParticipantImpl` downcasts** exist in non-test code — the interface
  route is safe, guarded by `var _ types.LocalParticipant = (*Participant)(nil)`.
- **Subscription never touches the publisher participant** beyond `HasPermission`:
  `ResolveMediaTrackForSubscriber` resolves through `room.trackManager`. So ~25 methods
  are real (identity/state/track registry/permissions/loop-safety), the rest no-op.
- **The publish pipeline needs no room edits**: firing
  `room.LocalParticipantListener().OnTrackPublished(p, track)` runs `trackManager.AddTrack`,
  the participant broadcast, and auto-subscribe of existing viewers — upstream code.
- **`MediaTrackReceiverParams.IsRelayed` finally earns its keep**: `relayrtc.MediaTrack`
  is a thin wrapper over `rtc.MediaTrackReceiver` + `SetupReceiver(syntheticReceiver)`.
- Join traps handled: `Verify()` must return true (1-minute join reaper), Room.Join needs
  a non-nil `routing.MessageSource` (`NewNullMessageSource`), `SubscriberAsPrimary` false
  skips the subscriber-PC path.

v1 identity model: one `__relay__` participant owns all relayed tracks (documented
ghosting trade-off). v2 — per-origin participants driven by mesh-relay's `relay/bus`
`ParticipantUpdate` state (landed there 2026-07-04) — reuses this primitive unchanged.

Dynacast note: `UpdateSubscribedQuality` is accepted and ignored in v1 (all relayed
layers flow); v2 maps it to `EdgeEngine.SetLayerDemand`.

### Edge side (DONE — v1)

The full announce path is wired:

```text
TrackUpdate ─► EdgeEngine ─► SyntheticReceiver ─► Announcer.OnRelayedTrack
  ─► getOrCreateRoom(TrackUpdate.room) ─► relayrtc.Participant (join once per room,
     NullMessageSource, AutoSubscribe=false) ─► relayrtc.MediaTrack ─► AddRelayedTrack
  ─► room.LocalParticipantListener().OnTrackPublished ─► trackManager/broadcast/auto-subscribe
```

- **Room resolution**: mesh-relay's `TrackUpdate` gained a `room` field (appended,
  wire-compatible) — the SDK-harness era carried it out-of-band in config. A roomless
  announcement is dropped loudly.
- **Retraction**: new `TrackClosed` protocol message; the origin sends it on unpublish,
  the edge engine closes the sink track, and `Announcer.OnRelayedTrackClosed` unpublishes
  the room track. Without it the synthetic publication outlived its source (frozen
  viewers). Link death (`OnEdgeLinkDown`) tears everything down; a reconnect re-announces
  under a fresh alias space (B7) and the announcer replaces publications idempotently.
- **Lifecycle**: the `__relay__` participant joins a room on first relayed track and is
  removed once it owns none, so empty rooms close normally.
- **Wiring** (`pkg/service/server.go`): with `relay.enabled`, the server hands the
  announcer `RoomManager.getOrCreateRoom` and the room-manager's receiver/subscriber
  configs, then registers the three agent callbacks.

Covered by `announcer_test.go` (publish/replace/retract/link-down/retire against a fake
room). NOT yet validated live — see below.

### Pre-Phase-5a investigation notes (kept for context)

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

### Live validation (2026-07-05) — first end-to-end run PASSED

Two fork nodes on loopback (A `peer_address` → B `listen_address: :7810`), demo H264
simulcast published to A via `lk`, subscriber joined B with `--auto-subscribe`:

```text
alice ─WebRTC→ A: RelayDownTrack (3 layers, native tap) ─QUIC→
B: SyntheticReceiver → __relay__ transport-less participant → room pipeline →
bob: "track subscribed {participant: __relay__, trackID: TR_..., kind: video}"
```

Observed: ~28k packets / ~30 MB relayed steady, `dgram_dropped_mtu: 0`,
`parse_errors: 0`, SR forwarding live (350+), cross-relay keyframes served, participant
visible via the standard API (`lk room participants list` → `__relay__ (ACTIVE) tracks: 1`).

**Bug found and fixed during the run — dynacast pause**: ~10 s after publish, node A told
alice's client to disable every simulcast layer ("no subscribers") and relayed media froze
at 2311 packets. `RelayDownTrack` attaches to the receiver directly, bypassing the
subscription accounting dynacast aggregates. Fix: on export, register relay demand via
`types.LocalMediaTrack.NotifySubscriberNodeMaxQuality` — the exact hook LiveKit Cloud's
closed relay drives through `UpdateSubscribedQuality`. v1 pins HIGH (all layers flow);
v2 maps live relay Subscribe masks. Cleared on unexport.

Still open before calling Phase F done:
- visual decode check (meet client) — RTP delivery + codec params proven, pixels not eyeballed
- simulcast quality switch under a real viewer; SR lip-sync measurement (audio+video)
- origin identity: `TrackUpdate.session_id` currently carries the participant SID
  (`PA_...`) rather than the identity (`alice`) — `streamIDOf` picks the stream id; fix
  alongside v2 per-origin participants

## Building

The mesh-relay module is private:

```bash
export GOPRIVATE=github.com/atdevten
gh auth setup-git       # or a PAT in ~/.netrc
go build ./...
```

CI (upstream-sync workflow) needs a `MESH_RELAY_TOKEN` repo secret with read access to
atdevten/mesh-relay.
