// Copyright 2026 mesh-relay authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package relay

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	meshrelay "github.com/atdevten/mesh-relay/relay"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"

	"github.com/livekit/livekit-server/pkg/config"
	"github.com/livekit/livekit-server/pkg/rtc/types"
)

// RelayIdentityPrefix marks participants owned by the relay itself. Tracks published
// under this prefix are never re-exported — the static-topology cycle guard until the
// Phase 4 hop-cap/cycle logic (guide §4.7) replaces it. The Phase 5a transport-less
// participant will announce relayed tracks under this identity.
const RelayIdentityPrefix = "__relay__"

// Service is the per-server relay runtime wrapper: it owns the Agent and supervises the
// static Phase F link(s) — dialing the peer with backoff on the origin side, accepting
// links on the edge side — for the lifetime of the server.
type Service struct {
	cfg   config.RelayConfig
	agent *Agent
	log   logger.Logger

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewService(cfg config.RelayConfig, nodeID livekit.NodeID) (*Service, error) {
	if cfg.ListenAddress == "" && cfg.PeerAddress == "" {
		return nil, errors.New("relay: enabled but neither listen_address nor peer_address is set")
	}
	log := logger.GetLogger().WithComponent("relay")
	agent := NewAgent(AgentParams{
		Logger: log,
		Config: meshrelay.DefaultConfig(),
		PeerID: livekit.ParticipantID(RelayIdentityPrefix + "-" + string(nodeID)),
	})
	return &Service{cfg: cfg, agent: agent, log: log}, nil
}

func (s *Service) Agent() *Agent { return s.agent }

// Start launches the link supervisors and registers the agent for the room-layer hooks.
func (s *Service) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	defaultAgent.Store(s.agent)

	if s.cfg.PeerAddress != "" {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.runOrigin(ctx)
		}()
	}
	if s.cfg.ListenAddress != "" {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.runEdge(ctx)
		}()
	}
	s.log.Infow("relay service started",
		"peerAddress", s.cfg.PeerAddress, "listenAddress", s.cfg.ListenAddress)
}

func (s *Service) Stop() {
	defaultAgent.CompareAndSwap(s.agent, nil)
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
	s.log.Infow("relay service stopped")
}

// runOrigin supervises the dial side: connect, run the origin engine until the link
// drops, back off, retry. Mirrors cmd/mesh-relay's supervisor.
func (s *Service) runOrigin(ctx context.Context) {
	backoff := time.Second
	const maxBackoff = 30 * time.Second
	for ctx.Err() == nil {
		peer, err := meshrelay.DialPeer(ctx, s.cfg.PeerAddress)
		if err != nil {
			s.log.Warnw("relay: dial peer failed; backing off", err,
				"peerAddress", s.cfg.PeerAddress, "backoff", backoff)
			if !sleepCtx(ctx, backoff) {
				return
			}
			backoff = min(backoff*2, maxBackoff)
			continue
		}
		backoff = time.Second
		s.log.Infow("relay: origin link up", "peerAddress", s.cfg.PeerAddress)

		runErr := s.agent.RunOrigin(ctx, peer)
		peer.Close()
		if ctx.Err() != nil {
			return
		}
		s.log.Warnw("relay: origin link ended; reconnecting", runErr, "backoff", backoff)
		if !sleepCtx(ctx, backoff) {
			return
		}
	}
}

// runEdge accepts peer links and runs an edge engine per link. Relayed tracks surface
// through Agent.OnRelayedTrack; until the Phase 5a transport-less participant lands the
// agent logs each one un-announced (see RELAY-FORK.md, edge side).
func (s *Service) runEdge(ctx context.Context) {
	ln, err := meshrelay.ListenPeer(s.cfg.ListenAddress)
	if err != nil {
		s.log.Errorw("relay: listen failed; edge side disabled", err,
			"listenAddress", s.cfg.ListenAddress)
		return
	}
	defer ln.Close()
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()
	s.log.Infow("relay: listening for peers", "listenAddress", s.cfg.ListenAddress)

	for ctx.Err() == nil {
		peer, err := ln.Accept(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.log.Warnw("relay: accept failed", err)
			continue
		}
		s.log.Infow("relay: peer connected", "remote", peer.RemoteAddr())
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer peer.Close()
			if err := s.agent.RunEdge(ctx, peer); err != nil && ctx.Err() == nil {
				s.log.Warnw("relay: edge link ended", err, "remote", peer.RemoteAddr())
			}
			// Every publication from this link is sourceless now; the room layer tears
			// them down. A reconnect re-announces under a fresh alias space (B7).
			s.agent.NotifyEdgeLinkDown()
		}()
	}
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	select {
	case <-time.After(d):
		return true
	case <-ctx.Done():
		return false
	}
}

// ---- room-layer hooks ----------------------------------------------------------------
//
// The room calls these package-level functions so the upstream edit in pkg/rtc/room.go
// stays a single line per call-site (pain point B12). They are nil-safe: with the relay
// disabled there is no registered agent and every call is a cheap no-op.

var defaultAgent atomic.Pointer[Agent]

// HandleTrackPublished exports a newly published local track over the relay link once its
// receiver is ready (AddOnReady fires immediately on live receivers, and on upgrade for
// pre-media DummyReceivers — before that, codec/layer info is incomplete). Tracks owned
// by the relay itself are skipped (cycle guard).
func HandleTrackPublished(room livekit.RoomName, participant types.Participant, track types.MediaTrack) {
	agent := defaultAgent.Load()
	if agent == nil {
		return
	}
	if strings.HasPrefix(string(participant.Identity()), RelayIdentityPrefix) {
		return
	}
	receivers := track.Receivers()
	if len(receivers) == 0 {
		return
	}
	recv := receivers[0] // primary codec; codec-fallback receivers are out of Phase F scope
	recv.AddOnReady(func() {
		if err := agent.ExportTrack(string(room), recv); err != nil {
			agent.logger.Warnw("relay: export track failed", err, "trackID", track.ID())
			return
		}
		// Register relay demand with dynacast, or it pauses the publisher's layers
		// ~10 s after publish when no LOCAL subscriber exists (RelayDownTrack attaches
		// to the receiver directly, bypassing the subscription layer dynacast counts).
		// This is the hook Cloud's closed relay drives via UpdateSubscribedQuality.
		// v1 pins HIGH (all layers flow to the relay); v2 maps live Subscribe masks.
		if lmt, ok := track.(types.LocalMediaTrack); ok {
			lmt.NotifySubscriberNodeMaxQuality(relayNodeID(agent), []types.SubscribedCodecQuality{
				{CodecMime: recv.Mime(), Quality: livekit.VideoQuality_HIGH},
			})
		}
	})
}

// relayNodeID labels the relay link as a subscriber "node" in dynacast bookkeeping.
func relayNodeID(agent *Agent) livekit.NodeID {
	return livekit.NodeID(agent.peerID)
}

// HandleTrackUnpublished stops relaying a track when its local publisher unpublishes.
func HandleTrackUnpublished(track types.MediaTrack) {
	if agent := defaultAgent.Load(); agent != nil {
		agent.UnexportTrack(track.ID())
		if lmt, ok := track.(types.LocalMediaTrack); ok {
			lmt.NotifySubscriberNodeMaxQuality(relayNodeID(agent), nil)
		}
	}
}
