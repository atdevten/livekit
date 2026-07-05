package relayrtc

import (
	"context"
	"sync"

	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"

	"github.com/livekit/livekit-server/pkg/relay"
	"github.com/livekit/livekit-server/pkg/routing"
	"github.com/livekit/livekit-server/pkg/rtc"
	"github.com/livekit/livekit-server/pkg/rtc/types"
)

// AnnouncerRoom is the slice of *rtc.Room the announcer needs — narrow for testability.
type AnnouncerRoom interface {
	Join(p types.LocalParticipant, requestSource routing.MessageSource, opts *rtc.ParticipantOptions, iceServers []*livekit.ICEServer) error
	GetParticipant(identity livekit.ParticipantIdentity) types.LocalParticipant
	RemoveParticipant(identity livekit.ParticipantIdentity, pID livekit.ParticipantID, reason types.ParticipantCloseReason)
	LocalParticipantListener() types.LocalParticipantListener
	ParticipantTelemetryListener() types.ParticipantTelemetryListener
}

var _ AnnouncerRoom = (*rtc.Room)(nil)

// Announcer is the edge-side room wiring (the piece Phase 5a unblocked): it turns each
// relayed SyntheticReceiver into a published track owned by a transport-less `__relay__`
// participant in the track's own room, so local clients discover and subscribe through
// the completely ordinary subscription path.
//
// v1 identity model: ONE relay participant per room owns every relayed track in it
// (temporary identity-ghosting, documented in RELAY-FORK.md); the original publisher
// identity survives in the track's stream/session metadata. v2 creates per-origin
// participants from mesh-relay's bus state and reuses the same primitive.
type Announcer struct {
	params AnnouncerParams

	mu    sync.Mutex
	rooms map[livekit.RoomName]*announcedRoom
}

type announcedRoom struct {
	room        AnnouncerRoom
	participant *Participant
	tracks      map[livekit.TrackID]*MediaTrack
}

type AnnouncerParams struct {
	// GetOrCreateRoom resolves (creating if needed) the local room a relayed track
	// belongs to. Wired to RoomManager by the server; a room with no local viewers yet
	// is created empty and fills as clients join.
	GetOrCreateRoom  func(ctx context.Context, name livekit.RoomName) (AnnouncerRoom, error)
	ReceiverConfig   rtc.ReceiverConfig
	SubscriberConfig rtc.DirectionConfig
	Logger           logger.Logger
}

func NewAnnouncer(params AnnouncerParams) *Announcer {
	if params.Logger == nil {
		params.Logger = logger.GetLogger()
	}
	return &Announcer{params: params, rooms: make(map[livekit.RoomName]*announcedRoom)}
}

// OnRelayedTrack publishes one relayed track into its room. Wire to Agent.OnRelayedTrack.
func (a *Announcer) OnRelayedTrack(recv *relay.SyntheticReceiver) {
	info := recv.Info()
	log := a.params.Logger.WithValues("trackID", info.TrackID, "room", info.Room, "origin", info.SessionID)
	if info.Room == "" {
		// Pre-room-field origins (or a misbehaving peer): without a room there is
		// nowhere to publish. Loud, per B1's "fail loudly, not as silent undecodable".
		log.Warnw("relay: relayed track carries no room; dropping announcement", nil)
		return
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	ar, err := a.ensureRoomLocked(livekit.RoomName(info.Room))
	if err != nil {
		log.Errorw("relay: resolve room for relayed track failed", err)
		return
	}

	trackID := livekit.TrackID(info.TrackID)
	if old, ok := ar.tracks[trackID]; ok {
		// Same track re-announced (link reconnect, fresh alias space — B7): replace the
		// publication so subscribers re-attach to the new receiver.
		ar.participant.RemoveRelayedTrack(old)
		old.Close(true)
		delete(ar.tracks, trackID)
	}

	mt := NewMediaTrack(MediaTrackParams{
		Receiver:          recv,
		Participant:       ar.participant,
		ReceiverConfig:    a.params.ReceiverConfig,
		SubscriberConfig:  a.params.SubscriberConfig,
		TelemetryListener: ar.room.ParticipantTelemetryListener(),
		Logger:            a.params.Logger,
	})
	ar.tracks[trackID] = mt
	ar.participant.AddRelayedTrack(mt)
	log.Infow("relay: relayed track published")
}

// OnRelayedTrackClosed tears one publication down (origin retracted the track). Wire to
// Agent.OnRelayedTrackClosed.
func (a *Announcer) OnRelayedTrackClosed(recv *relay.SyntheticReceiver) {
	info := recv.Info()
	a.mu.Lock()
	defer a.mu.Unlock()
	ar, ok := a.rooms[livekit.RoomName(info.Room)]
	if !ok {
		return
	}
	trackID := livekit.TrackID(info.TrackID)
	mt, ok := ar.tracks[trackID]
	if !ok {
		return
	}
	delete(ar.tracks, trackID)
	ar.participant.RemoveRelayedTrack(mt)
	mt.Close(false)
	a.params.Logger.Infow("relay: relayed track unpublished", "trackID", trackID, "room", info.Room)
	a.maybeRetireRoomLocked(livekit.RoomName(info.Room), ar)
}

// LinkDown tears down every relayed publication — the relay link died, so all synthetic
// tracks are sourceless. Wire to Agent.OnEdgeLinkDown.
func (a *Announcer) LinkDown() {
	a.mu.Lock()
	defer a.mu.Unlock()
	for name, ar := range a.rooms {
		for id, mt := range ar.tracks {
			ar.participant.RemoveRelayedTrack(mt)
			mt.Close(false)
			delete(ar.tracks, id)
		}
		a.maybeRetireRoomLocked(name, ar)
	}
}

// ensureRoomLocked resolves the room and joins its relay participant on first use.
func (a *Announcer) ensureRoomLocked(name livekit.RoomName) (*announcedRoom, error) {
	if ar, ok := a.rooms[name]; ok {
		return ar, nil
	}
	room, err := a.params.GetOrCreateRoom(context.Background(), name)
	if err != nil {
		return nil, err
	}

	identity := livekit.ParticipantIdentity(relay.RelayIdentityPrefix)
	p := NewParticipant(ParticipantParams{
		Identity:          identity,
		Listener:          room.LocalParticipantListener(),
		TelemetryListener: room.ParticipantTelemetryListener(),
		Logger:            a.params.Logger,
	})
	// The null source keeps Room's request-source bookkeeping non-nil (its Replace path
	// closes the stored source); AutoSubscribe=false keeps the room from subscribing the
	// relay participant to local tracks — it has no subscriber side.
	err = room.Join(p, routing.NewNullMessageSource(livekit.ConnectionID(p.ID())),
		&rtc.ParticipantOptions{AutoSubscribe: false}, nil)
	if err != nil {
		return nil, err
	}

	ar := &announcedRoom{room: room, participant: p, tracks: make(map[livekit.TrackID]*MediaTrack)}
	a.rooms[name] = ar
	a.params.Logger.Infow("relay: participant joined room", "room", name, "pID", p.ID())
	return ar, nil
}

// maybeRetireRoomLocked removes the relay participant once it owns no tracks, so an
// otherwise-empty room can close normally.
func (a *Announcer) maybeRetireRoomLocked(name livekit.RoomName, ar *announcedRoom) {
	if len(ar.tracks) > 0 {
		return
	}
	delete(a.rooms, name)
	ar.room.RemoveParticipant(ar.participant.Identity(), ar.participant.ID(),
		types.ParticipantCloseReasonServiceRequestRemoveParticipant)
}
