package relayrtc

import (
	"testing"

	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"

	meshrelay "github.com/atdevten/mesh-relay/relay"

	"github.com/livekit/livekit-server/pkg/relay"
	"github.com/livekit/livekit-server/pkg/rtc"
	"github.com/livekit/livekit-server/pkg/rtc/types"
)

// fakeListener records the publish-pipeline callbacks the room would receive.
type fakeListener struct {
	types.LocalParticipantListener
	published   []types.MediaTrack
	unpublished []types.MediaTrack
}

func (l *fakeListener) OnTrackPublished(_ types.Participant, track types.MediaTrack) {
	l.published = append(l.published, track)
}

func (l *fakeListener) OnTrackUnpublished(_ types.Participant, track types.MediaTrack) {
	l.unpublished = append(l.unpublished, track)
}

func newTestReceiver(t *testing.T) *relay.SyntheticReceiver {
	t.Helper()
	recv, err := relay.NewSyntheticReceiver(meshrelay.TrackInfo{
		SessionID:   "alice",
		TrackID:     "TR_relay1",
		Kind:        meshrelay.KindVideo,
		CodecMime:   "video/VP8",
		ClockRate:   90000,
		PayloadType: 96,
		Layers:      1,
		SSRCs:       []uint32{0x1234},
	}, logger.GetLogger())
	if err != nil {
		t.Fatalf("synthetic receiver: %v", err)
	}
	return recv
}

// TestRelayParticipantPublishLifecycle: the Phase 5a core loop — a transport-less
// participant publishes a relayed track through the room's listener pipeline, exposes it
// to the subscription surface, and tears down cleanly.
func TestRelayParticipantPublishLifecycle(t *testing.T) {
	listener := &fakeListener{}
	p := NewParticipant(ParticipantParams{
		Identity: "__relay__",
		Listener: listener,
	})

	if !p.Verify() || !p.IsReady() || p.State() != livekit.ParticipantInfo_ACTIVE {
		t.Fatal("participant must be born verified, ready, ACTIVE")
	}
	if p.ID() == "" {
		t.Fatal("missing generated SID")
	}

	track := NewMediaTrack(MediaTrackParams{
		Receiver:         newTestReceiver(t),
		Participant:      p,
		ReceiverConfig:   rtc.ReceiverConfig{PacketBufferSizeVideo: 500, PacketBufferSizeAudio: 500},
		SubscriberConfig: rtc.DirectionConfig{},
		Logger:           logger.GetLogger(),
	})

	if track.ID() != "TR_relay1" {
		t.Fatalf("track id: %q", track.ID())
	}
	if track.PublisherIdentity() != "__relay__" || track.PublisherID() != p.ID() {
		t.Fatalf("publisher identity not threaded: %s/%s", track.PublisherIdentity(), track.PublisherID())
	}
	if len(track.Receivers()) == 0 {
		t.Fatal("synthetic receiver not attached")
	}

	p.AddRelayedTrack(track)
	if len(listener.published) != 1 || listener.published[0].ID() != track.ID() {
		t.Fatalf("publish pipeline not fired: %+v", listener.published)
	}
	if got := p.GetPublishedTrack(track.ID()); got != types.MediaTrack(track) {
		t.Fatal("track not resolvable on participant")
	}
	if !p.HasPermission(track.ID(), "bob") {
		t.Fatal("subscribers must have permission")
	}

	pi := p.ToProto()
	if !pi.IsPublisher || len(pi.Tracks) != 1 || pi.Tracks[0].Sid != "TR_relay1" {
		t.Fatalf("proto snapshot wrong: %+v", pi)
	}

	// Origin retracts the track: unpublish flows through the listener.
	p.RemoveRelayedTrack(track)
	if len(listener.unpublished) != 1 {
		t.Fatal("unpublish pipeline not fired")
	}
	if p.GetPublishedTrack(track.ID()) != nil {
		t.Fatal("track survived removal")
	}

	// Close is idempotent and flips disconnect surfaces.
	closedCb := false
	p.AddOnClose("test", func(types.LocalParticipant) { closedCb = true })
	if err := p.Close(false, types.ParticipantCloseReasonNone, false); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(false, types.ParticipantCloseReasonNone, false); err != nil {
		t.Fatal(err)
	}
	if !closedCb || !p.IsClosed() || !p.IsDisconnected() {
		t.Fatal("close state wrong")
	}
	select {
	case <-p.Disconnected():
	default:
		t.Fatal("Disconnected channel not closed")
	}
}
