package relay

import (
	"context"
	"sync"

	meshrelay "github.com/atdevten/mesh-relay/relay"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"github.com/pion/rtp"

	"github.com/livekit/livekit-server/pkg/sfu"
)

// Agent is the per-node relay runtime. It owns the QUIC link(s) to peer servers and the
// mesh-relay engines, and exposes the two hooks the room layer calls:
//
//	ExportTrack(receiver)  — origin side: start relaying a locally published track to the
//	                         peer. Called from the track-published path.
//	UnexportTrack(trackID) — origin side: stop when the local publisher unpublishes.
//
// and one callback the room layer provides:
//
//	OnRelayedTrack(fn)     — edge side: a remote track arrived; fn must announce the
//	                         *SyntheticReceiver as a published track (create the remote
//	                         participant if needed) so local clients can subscribe.
//
// Phase F scope: one static peer per agent (mirror of mesh-relay's Phase 1 topology).
// Discovery/bus (Phases 4–5) replace the static peer with roster-driven links.
type Agent struct {
	logger logger.Logger
	cfg    meshrelay.Config
	peerID livekit.ParticipantID

	mu            sync.Mutex
	exported      map[string]*RelayDownTrack // by track id
	pending       []meshrelay.SourceTrack    // exported, not yet announced on the current link
	closedPending []string                   // unexported, not yet retired on the current link
	kick          chan struct{}              // wakes agentSource.Run after a pending change

	onRelayedTrack       func(*SyntheticReceiver)
	onRelayedTrackClosed func(*SyntheticReceiver)
	onEdgeLinkDown       func()
}

type AgentParams struct {
	Logger logger.Logger
	Config meshrelay.Config
	// PeerID labels this relay link inside SFU bookkeeping (downtrack subscriber id).
	PeerID livekit.ParticipantID
}

func NewAgent(p AgentParams) *Agent {
	return &Agent{
		logger:   p.Logger,
		cfg:      p.Config,
		peerID:   p.PeerID,
		exported: make(map[string]*RelayDownTrack),
		kick:     make(chan struct{}, 1),
	}
}

// OnRelayedTrack registers the edge-side announcement callback. Must be set before
// RunEdge accepts links.
func (a *Agent) OnRelayedTrack(fn func(*SyntheticReceiver)) { a.onRelayedTrack = fn }

// OnRelayedTrackClosed registers the retraction callback: the origin sent TrackClosed
// (its publisher unpublished), so the local synthetic publication must be torn down.
func (a *Agent) OnRelayedTrackClosed(fn func(*SyntheticReceiver)) { a.onRelayedTrackClosed = fn }

// OnEdgeLinkDown registers the link-death callback: every relayed publication from that
// link is now sourceless and must be torn down (a reconnect re-announces from scratch
// under a fresh alias space, B7).
func (a *Agent) OnEdgeLinkDown(fn func()) { a.onEdgeLinkDown = fn }

// NotifyEdgeLinkDown is called by the Service supervisor after an edge link ends.
func (a *Agent) NotifyEdgeLinkDown() {
	if fn := a.onEdgeLinkDown; fn != nil {
		fn()
	}
}

// ---- origin side -------------------------------------------------------------------------

// ExportTrack starts relaying a locally published track. Idempotent per track id.
// Never blocks: announcement is queued and drained by the link's source loop, so the
// room's publish path is safe to call this even while the peer link is down.
func (a *Agent) ExportTrack(room string, receiver sfu.TrackReceiver) error {
	a.mu.Lock()
	id := string(receiver.TrackID())
	if _, ok := a.exported[id]; ok {
		a.mu.Unlock()
		return nil
	}
	rdt := NewRelayDownTrack(receiver, room, a.peerID, a.logger)
	if err := receiver.AddDownTrack(rdt); err != nil {
		a.mu.Unlock()
		return err
	}
	a.exported[id] = rdt
	a.pending = append(a.pending, rdt)
	a.mu.Unlock()
	a.wake()
	a.logger.Infow("relay: track exported", "trackID", id)
	return nil
}

// UnexportTrack stops relaying a track (local publisher unpublished).
func (a *Agent) UnexportTrack(trackID livekit.TrackID) {
	id := string(trackID)
	a.mu.Lock()
	rdt := a.exported[id]
	delete(a.exported, id)
	if rdt != nil {
		// If still queued un-announced, retract it; the retire below is then a no-op
		// on the engine side (unknown ids are ignored).
		for i, st := range a.pending {
			if st == meshrelay.SourceTrack(rdt) {
				a.pending = append(a.pending[:i], a.pending[i+1:]...)
				break
			}
		}
		a.closedPending = append(a.closedPending, id)
	}
	a.mu.Unlock()
	if rdt == nil {
		return
	}
	rdt.Close()
	a.wake()
	a.logger.Infow("relay: track unexported", "trackID", trackID)
}

// wake nudges the source loop; a full kick channel already guarantees a re-drain.
func (a *Agent) wake() {
	select {
	case a.kick <- struct{}{}:
	default:
	}
}

// RunOrigin drives the origin engine over an established peer link. It blocks until the
// link drops or ctx is cancelled; the caller supervises reconnection. Every currently
// exported track is (re-)announced on the new link before new exports flow.
func (a *Agent) RunOrigin(ctx context.Context, peer *meshrelay.PeerConn) error {
	a.resetPendingForNewLink()
	eng := meshrelay.NewOriginEngine(a.cfg, zapFrom(a.logger), agentSource{a}, peer)
	return eng.Run(ctx)
}

// resetPendingForNewLink queues every exported track for (re-)announcement — a fresh link
// starts from a clean announce state, so retirements queued for the old link are moot.
func (a *Agent) resetPendingForNewLink() {
	a.mu.Lock()
	a.pending = a.pending[:0]
	for _, rdt := range a.exported {
		a.pending = append(a.pending, rdt)
	}
	a.closedPending = nil
	a.mu.Unlock()
}

// agentSource adapts the Agent's export stream to mesh-relay's Source seam.
type agentSource struct{ a *Agent }

func (s agentSource) Run(ctx context.Context, onTrack func(meshrelay.SourceTrack), onClosed func(string)) error {
	for {
		s.a.mu.Lock()
		pending := s.a.pending
		closed := s.a.closedPending
		s.a.pending = nil
		s.a.closedPending = nil
		s.a.mu.Unlock()
		for _, st := range pending {
			onTrack(st)
		}
		for _, id := range closed {
			onClosed(id)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.a.kick:
		}
	}
}

func (s agentSource) Close() error { return nil }

// ---- edge side ---------------------------------------------------------------------------

// RunEdge drives the edge engine over an accepted peer link. Each TrackUpdate becomes a
// SyntheticReceiver handed to the OnRelayedTrack callback.
func (a *Agent) RunEdge(ctx context.Context, peer *meshrelay.PeerConn) error {
	eng := meshrelay.NewEdgeEngine(a.cfg, zapFrom(a.logger), agentSink{a}, peer)
	return eng.Run(ctx)
}

// agentSink adapts SyntheticReceiver construction to mesh-relay's Sink seam.
type agentSink struct{ a *Agent }

func (s agentSink) NewTrack(info meshrelay.TrackInfo) (meshrelay.SinkTrack, error) {
	recv, err := NewSyntheticReceiver(info, s.a.logger)
	if err != nil {
		return nil, err
	}
	if fn := s.a.onRelayedTrack; fn != nil {
		fn(recv)
	} else {
		s.a.logger.Warnw("relay: no OnRelayedTrack callback; relayed track not announced", nil,
			"trackID", info.TrackID)
	}
	return notifyingSinkTrack{inner: recv.SinkTrack(), recv: recv, a: s.a}, nil
}

// notifyingSinkTrack wraps the receiver's sink so a TrackClosed retraction (edge engine
// calls Close) also reaches the room layer. It re-implements SenderReportSink explicitly:
// embedding the SinkTrack interface value would hide the concrete type's optional
// interface from the engine's type assertion (B4 would silently stop working).
type notifyingSinkTrack struct {
	inner meshrelay.SinkTrack
	recv  *SyntheticReceiver
	a     *Agent
}

var (
	_ meshrelay.SinkTrack        = notifyingSinkTrack{}
	_ meshrelay.SenderReportSink = notifyingSinkTrack{}
)

func (t notifyingSinkTrack) WriteRTP(layer uint8, pkt *rtp.Packet) error {
	return t.inner.WriteRTP(layer, pkt)
}

func (t notifyingSinkTrack) KeyFrameRequests() <-chan uint8 { return t.inner.KeyFrameRequests() }

func (t notifyingSinkTrack) WriteSenderReport(payload []byte) error {
	if sr, ok := t.inner.(meshrelay.SenderReportSink); ok {
		return sr.WriteSenderReport(payload)
	}
	return nil
}

func (t notifyingSinkTrack) Close() {
	t.inner.Close()
	if fn := t.a.onRelayedTrackClosed; fn != nil {
		fn(t.recv)
	}
}

func (s agentSink) Close() error { return nil }
