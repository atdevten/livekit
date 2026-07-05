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
	"testing"
	"time"

	"github.com/pion/webrtc/v4"

	meshrelay "github.com/atdevten/mesh-relay/relay"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"

	"github.com/livekit/livekit-server/pkg/sfu"
)

// fakeReceiver stubs only what the export path touches; the embedded nil interface
// panics loudly if the agent starts depending on more of sfu.TrackReceiver.
type fakeReceiver struct {
	sfu.TrackReceiver
	id         livekit.TrackID
	downTracks int
}

func (f *fakeReceiver) TrackID() livekit.TrackID { return f.id }
func (f *fakeReceiver) StreamID() string         { return string(f.id) + "_stream" }
func (f *fakeReceiver) AddDownTrack(sfu.TrackSender) error {
	f.downTracks++
	return nil
}
func (f *fakeReceiver) Codec() webrtc.RTPCodecParameters {
	return webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{MimeType: "video/VP8", ClockRate: 90000},
		PayloadType:        96,
	}
}
func (f *fakeReceiver) TrackInfo() *livekit.TrackInfo {
	return &livekit.TrackInfo{Sid: string(f.id), Type: livekit.TrackType_VIDEO}
}

func newTestAgent() *Agent {
	return NewAgent(AgentParams{
		Logger: logger.GetLogger(),
		Config: meshrelay.DefaultConfig(),
		PeerID: "PA_relay_test",
	})
}

// drainSource runs agentSource.Run and forwards announcements/retirements to channels.
func drainSource(ctx context.Context, a *Agent) (<-chan string, <-chan string) {
	announced := make(chan string, 16)
	retired := make(chan string, 16)
	go func() {
		_ = agentSource{a}.Run(ctx,
			func(st meshrelay.SourceTrack) { announced <- st.Info().TrackID },
			func(id string) { retired <- id },
		)
	}()
	return announced, retired
}

func recvOne(t *testing.T, ch <-chan string, what string) string {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		return ""
	}
}

func expectNone(t *testing.T, ch <-chan string, what string) {
	t.Helper()
	select {
	case v := <-ch:
		t.Fatalf("unexpected %s: %q", what, v)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestExportAnnounceAndRetire(t *testing.T) {
	a := newTestAgent()
	recvA := &fakeReceiver{id: "TR_a"}

	// Exported before the link is up: must not block, must queue.
	if err := a.ExportTrack("room", recvA); err != nil {
		t.Fatal(err)
	}
	if err := a.ExportTrack("room", recvA); err != nil { // idempotent per track id
		t.Fatal(err)
	}
	if recvA.downTracks != 1 {
		t.Fatalf("downtrack registered %d times, want 1", recvA.downTracks)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	announced, retired := drainSource(ctx, a)

	if id := recvOne(t, announced, "pre-link announce"); id != "TR_a" {
		t.Fatalf("announced %q, want TR_a", id)
	}
	expectNone(t, announced, "duplicate announce")

	// Exported while the link is live.
	recvB := &fakeReceiver{id: "TR_b"}
	if err := a.ExportTrack("room", recvB); err != nil {
		t.Fatal(err)
	}
	if id := recvOne(t, announced, "live announce"); id != "TR_b" {
		t.Fatalf("announced %q, want TR_b", id)
	}

	// Unpublish retires the track on the link and closes the downtrack.
	a.UnexportTrack("TR_a")
	if id := recvOne(t, retired, "retire"); id != "TR_a" {
		t.Fatalf("retired %q, want TR_a", id)
	}
	a.mu.Lock()
	_, stillExported := a.exported["TR_a"]
	a.mu.Unlock()
	if stillExported {
		t.Fatal("TR_a still in exported set after unexport")
	}

	a.UnexportTrack("TR_missing") // unknown id: no-op, no retire emitted
	expectNone(t, retired, "retire for unknown id")
}

func TestUnexportBeforeLinkRetractsPending(t *testing.T) {
	a := newTestAgent()
	if err := a.ExportTrack("room", &fakeReceiver{id: "TR_gone"}); err != nil {
		t.Fatal(err)
	}
	a.UnexportTrack("TR_gone")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	announced, _ := drainSource(ctx, a)

	// Retracted before any link consumed it: never announced.
	expectNone(t, announced, "announce of retracted track")
}

func TestReconnectReannouncesExportedTracks(t *testing.T) {
	a := newTestAgent()
	if err := a.ExportTrack("room", &fakeReceiver{id: "TR_a"}); err != nil {
		t.Fatal(err)
	}
	if err := a.ExportTrack("room", &fakeReceiver{id: "TR_b"}); err != nil {
		t.Fatal(err)
	}

	// First link consumes the queue…
	ctx1, cancel1 := context.WithCancel(context.Background())
	announced1, _ := drainSource(ctx1, a)
	got := map[string]bool{
		recvOne(t, announced1, "first-link announce"): true,
	}
	got[recvOne(t, announced1, "first-link announce")] = true
	if !got["TR_a"] || !got["TR_b"] {
		t.Fatalf("first link announced %v, want TR_a and TR_b", got)
	}
	cancel1()

	// …then drops; a stale retirement queued mid-gap must not leak to the new link.
	a.UnexportTrack("TR_b")
	a.resetPendingForNewLink() // what RunOrigin does before starting the engine

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	announced2, retired2 := drainSource(ctx2, a)

	if id := recvOne(t, announced2, "re-announce"); id != "TR_a" {
		t.Fatalf("re-announced %q, want TR_a (TR_b was unexported)", id)
	}
	expectNone(t, announced2, "extra re-announce")
	expectNone(t, retired2, "stale retirement on new link")
}
