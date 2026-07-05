package relayrtc

import (
	"context"
	"testing"

	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"

	meshrelay "github.com/atdevten/mesh-relay/relay"

	"github.com/livekit/livekit-server/pkg/relay"
	"github.com/livekit/livekit-server/pkg/routing"
	"github.com/livekit/livekit-server/pkg/rtc"
	"github.com/livekit/livekit-server/pkg/rtc/types"
)

// fakeRoom records the Room interactions the announcer drives.
type fakeRoom struct {
	name     livekit.RoomName
	listener *fakeListener
	joined   []types.LocalParticipant
	removed  []livekit.ParticipantIdentity
}

func (r *fakeRoom) Join(p types.LocalParticipant, src routing.MessageSource, opts *rtc.ParticipantOptions, _ []*livekit.ICEServer) error {
	if src == nil {
		panic("nil requestSource would nil-deref in Room.ReplaceParticipantRequestSource")
	}
	if opts == nil || opts.AutoSubscribe {
		panic("relay participant must join with AutoSubscribe=false")
	}
	r.joined = append(r.joined, p)
	return nil
}

func (r *fakeRoom) GetParticipant(identity livekit.ParticipantIdentity) types.LocalParticipant {
	for _, p := range r.joined {
		if p.Identity() == identity {
			return p
		}
	}
	return nil
}

func (r *fakeRoom) RemoveParticipant(identity livekit.ParticipantIdentity, _ livekit.ParticipantID, _ types.ParticipantCloseReason) {
	r.removed = append(r.removed, identity)
}

func (r *fakeRoom) LocalParticipantListener() types.LocalParticipantListener { return r.listener }
func (r *fakeRoom) ParticipantTelemetryListener() types.ParticipantTelemetryListener {
	return nil
}

func testAnnouncer(t *testing.T) (*Announcer, map[livekit.RoomName]*fakeRoom) {
	t.Helper()
	rooms := make(map[livekit.RoomName]*fakeRoom)
	a := NewAnnouncer(AnnouncerParams{
		GetOrCreateRoom: func(_ context.Context, name livekit.RoomName) (AnnouncerRoom, error) {
			if r, ok := rooms[name]; ok {
				return r, nil
			}
			r := &fakeRoom{name: name, listener: &fakeListener{}}
			rooms[name] = r
			return r, nil
		},
		ReceiverConfig: rtc.ReceiverConfig{PacketBufferSizeVideo: 500, PacketBufferSizeAudio: 500},
		Logger:         logger.GetLogger(),
	})
	return a, rooms
}

func relayedReceiver(t *testing.T, room, trackID string) *relay.SyntheticReceiver {
	t.Helper()
	recv, err := relay.NewSyntheticReceiver(meshrelay.TrackInfo{
		SessionID: "alice", Room: room, TrackID: trackID, Kind: meshrelay.KindVideo,
		CodecMime: "video/VP8", ClockRate: 90000, PayloadType: 96, Layers: 1,
		SSRCs: []uint32{0xAB},
	}, logger.GetLogger())
	if err != nil {
		t.Fatal(err)
	}
	return recv
}

// TestAnnouncerPublishesIntoTrackRoom: the full edge announce path — room resolved from
// TrackUpdate.room, relay participant joined once, track published via the listener
// pipeline, retraction unpublishes, empty participant retired from the room.
func TestAnnouncerPublishesIntoTrackRoom(t *testing.T) {
	a, rooms := testAnnouncer(t)

	recv := relayedReceiver(t, "conf-1", "TR1")
	a.OnRelayedTrack(recv)

	room := rooms["conf-1"]
	if room == nil || len(room.joined) != 1 {
		t.Fatalf("relay participant not joined: %+v", rooms)
	}
	p := room.joined[0]
	if p.Identity() != relay.RelayIdentityPrefix {
		t.Fatalf("identity: %s", p.Identity())
	}
	if len(room.listener.published) != 1 || room.listener.published[0].ID() != "TR1" {
		t.Fatalf("publish pipeline not fired: %+v", room.listener.published)
	}
	if p.GetPublishedTrack("TR1") == nil {
		t.Fatal("track not on participant")
	}

	// Second track, same room: same participant, no second join.
	a.OnRelayedTrack(relayedReceiver(t, "conf-1", "TR2"))
	if len(room.joined) != 1 {
		t.Fatal("joined twice for one room")
	}
	if len(room.listener.published) != 2 {
		t.Fatal("second track not published")
	}

	// Reconnect re-announce of TR1: replaced, not duplicated.
	a.OnRelayedTrack(relayedReceiver(t, "conf-1", "TR1"))
	if len(room.listener.unpublished) != 1 {
		t.Fatalf("old publication not retracted on re-announce: %+v", room.listener.unpublished)
	}
	if got := len(p.GetPublishedTracks()); got != 2 {
		t.Fatalf("want 2 live tracks after replace, got %d", got)
	}

	// Origin retracts both: participant retired from the room once empty.
	a.OnRelayedTrackClosed(relayedReceiver(t, "conf-1", "TR1"))
	a.OnRelayedTrackClosed(relayedReceiver(t, "conf-1", "TR2"))
	if len(room.removed) != 1 || room.removed[0] != relay.RelayIdentityPrefix {
		t.Fatalf("relay participant not retired: %+v", room.removed)
	}
}

func TestAnnouncerDropsRoomlessTrack(t *testing.T) {
	a, rooms := testAnnouncer(t)
	a.OnRelayedTrack(relayedReceiver(t, "", "TR1"))
	if len(rooms) != 0 {
		t.Fatal("roomless track must not create a room")
	}
}

func TestAnnouncerLinkDownTearsEverythingDown(t *testing.T) {
	a, rooms := testAnnouncer(t)
	a.OnRelayedTrack(relayedReceiver(t, "conf-1", "TR1"))
	a.OnRelayedTrack(relayedReceiver(t, "conf-2", "TR2"))

	a.LinkDown()

	for name, room := range rooms {
		if len(room.listener.unpublished) != 1 {
			t.Fatalf("room %s: track not unpublished on link down", name)
		}
		if len(room.removed) != 1 {
			t.Fatalf("room %s: participant not retired on link down", name)
		}
	}
	// A fresh announce after link death must re-join cleanly.
	a.OnRelayedTrack(relayedReceiver(t, "conf-1", "TR1"))
	if len(rooms["conf-1"].joined) != 2 {
		t.Fatal("re-announce after link down did not rejoin")
	}
}
