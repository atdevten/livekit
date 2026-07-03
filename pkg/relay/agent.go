package relay

import (
	"context"
	"sync"

	meshrelay "github.com/atdevten/mesh-relay/relay"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"

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

	mu             sync.Mutex
	exported       map[string]*RelayDownTrack // by track id
	exportCh       chan meshrelay.SourceTrack
	closedCh       chan string
	onRelayedTrack func(*SyntheticReceiver)

	ctx    context.Context
	cancel context.CancelFunc
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
		exportCh: make(chan meshrelay.SourceTrack, 16),
		closedCh: make(chan string, 16),
	}
}

// OnRelayedTrack registers the edge-side announcement callback. Must be set before
// RunEdge accepts links.
func (a *Agent) OnRelayedTrack(fn func(*SyntheticReceiver)) { a.onRelayedTrack = fn }

// ---- origin side -------------------------------------------------------------------------

// ExportTrack starts relaying a locally published track. Idempotent per track id.
func (a *Agent) ExportTrack(receiver sfu.TrackReceiver) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	id := string(receiver.TrackID())
	if _, ok := a.exported[id]; ok {
		return nil
	}
	rdt := NewRelayDownTrack(receiver, a.peerID, a.logger)
	if err := receiver.AddDownTrack(rdt); err != nil {
		return err
	}
	a.exported[id] = rdt
	a.exportCh <- rdt
	a.logger.Infow("relay: track exported", "trackID", id)
	return nil
}

// UnexportTrack stops relaying a track (local publisher unpublished).
func (a *Agent) UnexportTrack(trackID livekit.TrackID) {
	a.mu.Lock()
	rdt := a.exported[string(trackID)]
	delete(a.exported, string(trackID))
	a.mu.Unlock()
	if rdt == nil {
		return
	}
	rdt.Close()
	a.closedCh <- string(trackID)
	a.logger.Infow("relay: track unexported", "trackID", trackID)
}

// RunOrigin drives the origin engine over an established peer link. It blocks until the
// link drops or ctx is cancelled; the caller supervises reconnection.
func (a *Agent) RunOrigin(ctx context.Context, peer *meshrelay.PeerConn) error {
	a.ctx, a.cancel = context.WithCancel(ctx)
	defer a.cancel()
	eng := meshrelay.NewOriginEngine(a.cfg, zapFrom(a.logger), agentSource{a}, peer)
	return eng.Run(a.ctx)
}

// agentSource adapts the Agent's export stream to mesh-relay's Source seam.
type agentSource struct{ a *Agent }

func (s agentSource) Run(ctx context.Context, onTrack func(meshrelay.SourceTrack), onClosed func(string)) error {
	// Replay tracks exported before the link came up (export order preserved by exportCh).
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case st := <-s.a.exportCh:
			onTrack(st)
		case id := <-s.a.closedCh:
			onClosed(id)
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
	return recv.SinkTrack(), nil
}

func (s agentSink) Close() error { return nil }
