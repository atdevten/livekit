// Package relayrtc bridges pkg/relay (the mesh-relay adapters) into the room layer.
// It lives outside pkg/relay because pkg/rtc already imports pkg/relay for the origin-side
// hooks; the edge side needs the opposite direction (this package imports pkg/rtc).
//
// Its centerpiece is the TRANSPORT-LESS PARTICIPANT — mesh-relay guide §4.1's
// Participant/LocalParticipant split (architecture §5.1), surfaced early as Phase 5a: a
// publisher with no PeerConnection so a relayed SyntheticReceiver is subscribable by local
// clients. OSS livekit has no such thing (types.LocalParticipant's only implementation is
// transport-bound; the IsRelayed flag is a Cloud vestige, never set true).
package relayrtc

import (
	"errors"
	"sync"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v4"
	"google.golang.org/protobuf/proto"

	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"github.com/livekit/protocol/observability/roomobs"
	"github.com/livekit/protocol/utils"

	"github.com/livekit/livekit-server/pkg/routing"
	"github.com/livekit/livekit-server/pkg/rtc/datatrack"
	"github.com/livekit/livekit-server/pkg/rtc/types"
	"github.com/livekit/livekit-server/pkg/sfu"
	"github.com/livekit/livekit-server/pkg/sfu/buffer"
	"github.com/livekit/livekit-server/pkg/sfu/pacer"
	"github.com/livekit/livekit-server/pkg/telemetry"
)

var errRelayParticipant = errors.New("relayrtc: not supported on a transport-less participant")

// Participant is a publisher with no WebRTC transport. It satisfies the full
// types.LocalParticipant surface so Room.Join and the subscription machinery treat it as
// any other participant, but every transport/signal/migration method is a no-op: media
// enters through SyntheticReceivers on its published tracks, not through a PeerConnection.
//
// Step-0 inventory (feasibility pass, 2026-07-04) — what the room actually calls on a
// PUBLISHER and therefore what is real here:
//   - identity/state: ID, Identity, State (ACTIVE), Kind, ToProto(+Version), IsReady
//   - track registry: GetPublishedTrack(s), RemovePublishedTrack, IsPublisher
//   - permissions: HasPermission (true for all subscribers), Hidden (false)
//   - join survival: Verify -> true (Room reaps unverified joins after 1 min),
//     SendJoinResponse -> nil, SubscriberAsPrimary/IsUsingSinglePeerConnection -> false
//   - room worker loops: GetAudioLevel -> (0,false), GetConnectionQuality -> nil
//     (call sites nil-guard), DebugInfo, GetLogger, Close/IsClosed/Disconnected
//
// Subscription resolution never touches the publisher participant beyond HasPermission —
// ResolveMediaTrackForSubscriber goes through the room's trackManager.
type Participant struct {
	params ParticipantParams

	lock        sync.RWMutex
	tracks      map[livekit.TrackID]types.MediaTrack
	closed      bool
	closeReason types.ParticipantCloseReason
	onClose     map[string]func(types.LocalParticipant)

	disconnected chan struct{}
	connectedAt  time.Time
	// protoVersion mirrors ParticipantImpl's monotonically bumped ParticipantInfo.Version;
	// version is the TimedVersion consumed by ToProtoWithVersion/Version.
	protoVersion uint32
	version      utils.TimedVersion

	telemetryGuard   *telemetry.ReferenceGuard
	grants           *auth.ClaimGrants
	logger           logger.Logger
	loggerResolver   logger.DeferredFieldResolver
	reporter         roomobs.ParticipantSessionReporter
	reporterResolver roomobs.ParticipantReporterResolver
}

type ParticipantParams struct {
	Identity livekit.ParticipantIdentity
	// SID defaults to a fresh PA_ guid.
	SID livekit.ParticipantID
	// Listener is room.LocalParticipantListener(); firing its OnTrackPublished is what
	// runs the room's untouched publish pipeline (trackManager.AddTrack, broadcast,
	// auto-subscribe) — no room edits needed for the edge announce path.
	Listener types.LocalParticipantListener
	// TelemetryListener is room.ParticipantTelemetryListener(); handed on to media tracks.
	TelemetryListener types.ParticipantTelemetryListener
	VersionGenerator  utils.TimedVersionGenerator
	Logger            logger.Logger
}

var _ types.LocalParticipant = (*Participant)(nil)

func NewParticipant(params ParticipantParams) *Participant {
	if params.SID == "" {
		params.SID = livekit.ParticipantID(utils.NewGuid(utils.ParticipantPrefix))
	}
	if params.VersionGenerator == nil {
		params.VersionGenerator = utils.NewDefaultTimedVersionGenerator()
	}
	if params.Logger == nil {
		params.Logger = logger.GetLogger()
	}
	log, logResolver := params.Logger.WithDeferredValues()
	reporter, repResolver := roomobs.DeferredParticipantReporter(roomobs.NewNoopProjectReporter())

	grants := &auth.ClaimGrants{
		Identity: string(params.Identity),
		Video:    &auth.VideoGrant{},
	}
	t := true
	f := false
	grants.Video.CanPublish = &t
	grants.Video.CanSubscribe = &f
	grants.Video.CanPublishData = &f

	return &Participant{
		params:           params,
		tracks:           make(map[livekit.TrackID]types.MediaTrack),
		onClose:          make(map[string]func(types.LocalParticipant)),
		disconnected:     make(chan struct{}),
		connectedAt:      time.Now(),
		version:          params.VersionGenerator.Next(),
		telemetryGuard:   &telemetry.ReferenceGuard{},
		grants:           grants,
		logger:           log,
		loggerResolver:   logResolver,
		reporter:         reporter,
		reporterResolver: repResolver,
	}
}

// ---- relay-side API (called by the edge wiring, not by the room) ------------------------

// AddRelayedTrack registers a published track and fires the room's publish pipeline.
func (p *Participant) AddRelayedTrack(track types.MediaTrack) {
	p.lock.Lock()
	if p.closed {
		p.lock.Unlock()
		return
	}
	p.tracks[track.ID()] = track
	p.version = p.params.VersionGenerator.Next()
	p.protoVersion++
	p.lock.Unlock()

	if p.params.Listener != nil {
		p.params.Listener.OnTrackPublished(p, track)
	}
}

// RemoveRelayedTrack unpublishes a track (relay link dropped or origin retracted it).
func (p *Participant) RemoveRelayedTrack(track types.MediaTrack) {
	p.lock.Lock()
	_, ok := p.tracks[track.ID()]
	delete(p.tracks, track.ID())
	p.version = p.params.VersionGenerator.Next()
	p.protoVersion++
	p.lock.Unlock()
	if !ok {
		return
	}
	if p.params.Listener != nil {
		p.params.Listener.OnTrackUnpublished(p, track)
	}
}

// ---- types.Participant -------------------------------------------------------------------

func (p *Participant) ID() livekit.ParticipantID             { return p.params.SID }
func (p *Participant) Identity() livekit.ParticipantIdentity { return p.params.Identity }

func (p *Participant) State() livekit.ParticipantInfo_State {
	if p.IsClosed() {
		return livekit.ParticipantInfo_DISCONNECTED
	}
	return livekit.ParticipantInfo_ACTIVE
}

func (p *Participant) ConnectedAt() time.Time { return p.connectedAt }

func (p *Participant) CloseReason() types.ParticipantCloseReason {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.closeReason
}

func (p *Participant) Kind() livekit.ParticipantInfo_Kind                { return livekit.ParticipantInfo_STANDARD }
func (p *Participant) KindDetails() []livekit.ParticipantInfo_KindDetail { return nil }
func (p *Participant) IsRecorder() bool                                  { return false }
func (p *Participant) IsDependent() bool                                 { return false }
func (p *Participant) IsAgent() bool                                     { return false }

func (p *Participant) GetLogger() logger.Logger { return p.logger }

func (p *Participant) CanSkipBroadcast() bool { return false }

func (p *Participant) Version() utils.TimedVersion {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.version
}

func (p *Participant) ToProto() *livekit.ParticipantInfo {
	pi, _ := p.ToProtoWithVersion()
	return pi
}

func (p *Participant) ToProtoWithVersion() (*livekit.ParticipantInfo, utils.TimedVersion) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	pi := &livekit.ParticipantInfo{
		Sid:         string(p.params.SID),
		Identity:    string(p.params.Identity),
		Name:        string(p.params.Identity),
		State:       livekit.ParticipantInfo_ACTIVE,
		JoinedAt:    p.connectedAt.Unix(),
		JoinedAtMs:  p.connectedAt.UnixMilli(),
		Version:     p.protoVersion,
		Permission:  p.grants.Video.ToPermission(),
		IsPublisher: true,
		Kind:        livekit.ParticipantInfo_STANDARD,
	}
	if p.closed {
		pi.State = livekit.ParticipantInfo_DISCONNECTED
	}
	for _, t := range p.tracks {
		pi.Tracks = append(pi.Tracks, t.ToProto())
	}
	return pi, p.version
}

func (p *Participant) IsPublisher() bool { return true }

func (p *Participant) GetPublishedTrack(trackID livekit.TrackID) types.MediaTrack {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.tracks[trackID]
}

func (p *Participant) GetPublishedTracks() []types.MediaTrack {
	p.lock.RLock()
	defer p.lock.RUnlock()
	out := make([]types.MediaTrack, 0, len(p.tracks))
	for _, t := range p.tracks {
		out = append(out, t)
	}
	return out
}

func (p *Participant) RemovePublishedTrack(track types.MediaTrack, isExpectedToResume bool) {
	p.lock.Lock()
	delete(p.tracks, track.ID())
	p.lock.Unlock()
	track.Close(isExpectedToResume)
}

func (p *Participant) GetPublishedDataTracks() []types.DataTrack           { return nil }
func (p *Participant) GetPublishedDataTrack(handle uint16) types.DataTrack { return nil }
func (p *Participant) RemovePublishedDataTrack(track types.DataTrack)      {}

func (p *Participant) GetAudioLevel() (float64, bool) { return 0, false }

// HasPermission: every local subscriber may subscribe to relayed tracks. Session-level
// ACLs stay client-side (JWT grants) per architecture §6 — nodes are mutually trusted.
func (p *Participant) HasPermission(livekit.TrackID, livekit.ParticipantIdentity) bool {
	return true
}

func (p *Participant) Hidden() bool { return false }

func (p *Participant) MigrateState() types.MigrateState { return types.MigrateStateComplete }

func (p *Participant) Close(sendLeave bool, reason types.ParticipantCloseReason, isExpectedToResume bool) error {
	p.lock.Lock()
	if p.closed {
		p.lock.Unlock()
		return nil
	}
	p.closed = true
	p.closeReason = reason
	tracks := make([]types.MediaTrack, 0, len(p.tracks))
	for _, t := range p.tracks {
		tracks = append(tracks, t)
	}
	p.tracks = make(map[livekit.TrackID]types.MediaTrack)
	callbacks := make([]func(types.LocalParticipant), 0, len(p.onClose))
	for _, cb := range p.onClose {
		callbacks = append(callbacks, cb)
	}
	p.lock.Unlock()

	close(p.disconnected)
	for _, t := range tracks {
		t.Close(isExpectedToResume)
	}
	for _, cb := range callbacks {
		cb(p)
	}
	return nil
}

func (p *Participant) IsClosed() bool {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.closed
}

func (p *Participant) IsDisconnected() bool { return p.IsClosed() }

func (p *Participant) SubscriptionPermission() (*livekit.SubscriptionPermission, utils.TimedVersion) {
	return nil, utils.TimedVersion(0)
}

func (p *Participant) UpdateSubscriptionPermission(
	*livekit.SubscriptionPermission,
	utils.TimedVersion,
	func(livekit.ParticipantID) types.LocalParticipant,
) error {
	return nil
}

func (p *Participant) DebugInfo() map[string]any {
	p.lock.RLock()
	defer p.lock.RUnlock()
	trackIDs := make([]livekit.TrackID, 0, len(p.tracks))
	for id := range p.tracks {
		trackIDs = append(trackIDs, id)
	}
	return map[string]any{
		"ID":       p.params.SID,
		"Identity": p.params.Identity,
		"Relay":    true,
		"Tracks":   trackIDs,
	}
}

func (p *Participant) HandleReceivedDataTrackMessage([]byte, *datatrack.Packet, int64) {}

func (p *Participant) GetParticipantListener() types.ParticipantListener { return p.params.Listener }

func (p *Participant) AddDataBlob(*livekit.DataBlob)                      {}
func (p *Participant) GetDataBlob(*livekit.DataBlobKey) *livekit.DataBlob { return nil }

// ---- types.LocalParticipant: getters ------------------------------------------------------

func (p *Participant) TelemetryGuard() *telemetry.ReferenceGuard { return p.telemetryGuard }
func (p *Participant) GetTelemetryListener() types.ParticipantTelemetryListener {
	return p.params.TelemetryListener
}

func (p *Participant) GetCountry() string                              { return "" }
func (p *Participant) GetTrailer() []byte                              { return nil }
func (p *Participant) GetLoggerResolver() logger.DeferredFieldResolver { return p.loggerResolver }
func (p *Participant) GetReporter() roomobs.ParticipantSessionReporter { return p.reporter }
func (p *Participant) GetReporterResolver() roomobs.ParticipantReporterResolver {
	return p.reporterResolver
}
func (p *Participant) GetAdaptiveStream() bool                        { return false }
func (p *Participant) GetEnableStartAtDesiredQuality() bool           { return false }
func (p *Participant) ProtocolVersion() types.ProtocolVersion         { return types.CurrentProtocol }
func (p *Participant) SupportsSyncStreamID() bool                     { return false }
func (p *Participant) SupportsTransceiverReuse(types.MediaTrack) bool { return false }
func (p *Participant) IsUsingSinglePeerConnection() bool              { return false }
func (p *Participant) IsReady() bool                                  { return !p.IsClosed() }
func (p *Participant) ActiveAt() time.Time                            { return p.connectedAt }
func (p *Participant) Disconnected() <-chan struct{}                  { return p.disconnected }
func (p *Participant) IsIdle() bool                                   { return false }
func (p *Participant) SubscriberAsPrimary() bool                      { return false }
func (p *Participant) GetClientInfo() *livekit.ClientInfo {
	return &livekit.ClientInfo{Sdk: livekit.ClientInfo_UNKNOWN}
}
func (p *Participant) GetClientConfiguration() *livekit.ClientConfiguration { return nil }
func (p *Participant) GetBufferFactory() *buffer.Factory                    { return nil }
func (p *Participant) GetPlayoutDelayConfig() *livekit.PlayoutDelay         { return nil }
func (p *Participant) GetPendingTrack(livekit.TrackID) *livekit.TrackInfo   { return nil }
func (p *Participant) GetICEConnectionInfo() []*types.ICEConnectionInfo     { return nil }
func (p *Participant) HasConnected() bool                                   { return true }
func (p *Participant) GetEnabledPublishCodecs() []*livekit.Codec            { return nil }
func (p *Participant) GetPublisherICESessionUfrag() (string, error) {
	return "", errRelayParticipant
}
func (p *Participant) SupportsMoving() error                          { return errRelayParticipant }
func (p *Participant) GetLastReliableSequence(migrateOut bool) uint32 { return 0 }

// ---- signal plumbing: no transport, all no-ops ---------------------------------------------

func (p *Participant) SwapResponseSink(routing.MessageSink, types.SignallingCloseReason) {}
func (p *Participant) GetResponseSink() routing.MessageSink                              { return nil }
func (p *Participant) CloseSignalConnection(types.SignallingCloseReason)                 {}
func (p *Participant) UpdateLastSeenSignal()                                             {}
func (p *Participant) SetSignalSourceValid(bool)                                         {}
func (p *Participant) HandleSignalSourceClose()                                          {}

// ---- updates -------------------------------------------------------------------------------

func (p *Participant) UpdateMetadata(*livekit.UpdateParticipantMetadata, bool) error { return nil }
func (p *Participant) SetName(string)                                                {}
func (p *Participant) SetMetadata(string)                                            {}
func (p *Participant) SetAttributes(map[string]string)                               {}
func (p *Participant) UpdateAudioTrack(*livekit.UpdateLocalAudioTrack) error         { return nil }
func (p *Participant) UpdateVideoTrack(*livekit.UpdateLocalVideoTrack) error         { return nil }

// ---- permissions ---------------------------------------------------------------------------

func (p *Participant) ClaimGrants() *auth.ClaimGrants                    { return p.grants.Clone() }
func (p *Participant) TokenExpiresAt() time.Time                         { return time.Time{} }
func (p *Participant) SetPermission(*livekit.ParticipantPermission) bool { return false }
func (p *Participant) CanPublish() bool                                  { return true }
func (p *Participant) CanPublishSource(livekit.TrackSource) bool         { return true }
func (p *Participant) CanSubscribe() bool                                { return false }
func (p *Participant) CanPublishData() bool                              { return false }

// ---- PeerConnection: none ------------------------------------------------------------------

func (p *Participant) HandleICETrickle(*livekit.TrickleRequest)      {}
func (p *Participant) HandleOffer(*livekit.SessionDescription) error { return errRelayParticipant }
func (p *Participant) GetAnswer() (webrtc.SessionDescription, uint32, error) {
	return webrtc.SessionDescription{}, 0, errRelayParticipant
}
func (p *Participant) HandleICETrickleSDPFragment(string) error { return errRelayParticipant }
func (p *Participant) HandleICERestartSDPFragment(string) (string, error) {
	return "", errRelayParticipant
}
func (p *Participant) AddTrack(*livekit.AddTrackRequest) {}
func (p *Participant) SetTrackMuted(*livekit.MuteTrackRequest, bool) *livekit.TrackInfo {
	return nil
}
func (p *Participant) HandleAnswer(*livekit.SessionDescription) {}
func (p *Participant) Negotiate(bool)                           {}
func (p *Participant) ICERestart(*livekit.ICEConfig)            {}
func (p *Participant) AddTrackLocal(webrtc.TrackLocal, types.AddTrackParams) (*webrtc.RTPSender, *webrtc.RTPTransceiver, error) {
	return nil, nil, errRelayParticipant
}
func (p *Participant) AddTransceiverFromTrackLocal(webrtc.TrackLocal, types.AddTrackParams) (*webrtc.RTPSender, *webrtc.RTPTransceiver, error) {
	return nil, nil, errRelayParticipant
}
func (p *Participant) RemoveTrackLocal(*webrtc.RTPSender) error { return errRelayParticipant }

func (p *Participant) WriteSubscriberRTCP([]rtcp.Packet) error { return nil }

// ---- subscriptions: this participant never subscribes ---------------------------------------

func (p *Participant) SubscribeToTrack(livekit.TrackID, bool)                                      {}
func (p *Participant) UnsubscribeFromTrack(livekit.TrackID)                                        {}
func (p *Participant) UpdateSubscribedTrackSettings(livekit.TrackID, *livekit.UpdateTrackSettings) {}
func (p *Participant) GetSubscribedTracks() []types.SubscribedTrack                                { return nil }
func (p *Participant) IsTrackNameSubscribed(livekit.ParticipantIdentity, string) bool              { return false }
func (p *Participant) SubscribeToDataTrack(livekit.TrackID)                                        {}
func (p *Participant) UnsubscribeFromDataTrack(livekit.TrackID)                                    {}
func (p *Participant) UpdateDataTrackSubscriptionOptions(livekit.TrackID, *livekit.DataTrackSubscriptionOptions) {
}

// Verify must return true: Room.Join arms a 1-minute reaper that removes any participant
// that has not "verified" (completed signal setup); a relay participant is born verified.
func (p *Participant) Verify() bool { return true }

func (p *Participant) VerifySubscribeParticipantInfo(livekit.ParticipantID, uint32) {}
func (p *Participant) WaitUntilSubscribed(time.Duration) error                      { return nil }
func (p *Participant) StopAndGetSubscribedTracksForwarderState() map[livekit.TrackID]*livekit.RTPForwarderState {
	return nil
}
func (p *Participant) SupportsCodecChange() bool { return false }

func (p *Participant) GetSubscribedParticipants() []livekit.ParticipantID { return nil }
func (p *Participant) IsSubscribedTo(livekit.ParticipantID) bool          { return false }

func (p *Participant) GetConnectionQuality() *livekit.ConnectionQualityInfo { return nil }

// ---- server-sent messages: nowhere to send them --------------------------------------------

func (p *Participant) SendJoinResponse(*livekit.JoinResponse) error           { return nil }
func (p *Participant) SendParticipantUpdate([]*livekit.ParticipantInfo) error { return nil }
func (p *Participant) SendSpeakerUpdate([]*livekit.SpeakerInfo, bool) error   { return nil }
func (p *Participant) SendDataMessage(livekit.DataPacket_Kind, []byte, livekit.ParticipantID, uint32) error {
	return nil
}
func (p *Participant) SendDataMessageUnlabeled([]byte, bool, livekit.ParticipantIdentity) error {
	return nil
}
func (p *Participant) SendRoomUpdate(*livekit.Room) error                                 { return nil }
func (p *Participant) SendConnectionQualityUpdate(*livekit.ConnectionQualityUpdate) error { return nil }
func (p *Participant) SendSubscriptionPermissionUpdate(livekit.ParticipantID, livekit.TrackID, bool) error {
	return nil
}
func (p *Participant) SendRefreshToken(string) error { return nil }
func (p *Participant) HandleReconnectAndSendResponse(livekit.ReconnectReason, *livekit.ReconnectResponse) error {
	return nil
}
func (p *Participant) IssueFullReconnect(types.ParticipantCloseReason)        {}
func (p *Participant) SendRoomMovedResponse(*livekit.RoomMovedResponse) error { return nil }
func (p *Participant) SendDataTrackSubscriberHandles(map[uint32]*livekit.DataTrackSubscriberHandles_PublishedDataTrack) error {
	return nil
}

// ---- lifecycle callbacks ---------------------------------------------------------------------

func (p *Participant) AddOnClose(key string, callback func(types.LocalParticipant)) {
	p.lock.Lock()
	defer p.lock.Unlock()
	if callback == nil {
		delete(p.onClose, key)
		return
	}
	p.onClose[key] = callback
}

func (p *Participant) OnClaimsChanged(func(types.LocalParticipant)) {}

func (p *Participant) HandleReceiverReport(*sfu.DownTrack, *rtcp.ReceiverReport) {}

// ---- session migration: not applicable --------------------------------------------------------

func (p *Participant) MaybeStartMigration(bool, func()) bool { return false }
func (p *Participant) NotifyMigration()                      {}
func (p *Participant) SetMigrateState(types.MigrateState)    {}
func (p *Participant) SetMigrateInfo(
	*webrtc.SessionDescription,
	*webrtc.SessionDescription,
	[]*livekit.TrackPublishedResponse,
	[]*livekit.DataChannelInfo,
	[]*livekit.DataChannelReceiveState,
	[]*livekit.PublishDataTrackResponse,
) {
}
func (p *Participant) IsReconnect() bool                 { return false }
func (p *Participant) MoveToRoom(types.MoveToRoomParams) {}

func (p *Participant) UpdateMediaRTT(uint32)     {}
func (p *Participant) UpdateSignalingRTT(uint32) {}

func (p *Participant) CacheDownTrack(livekit.TrackID, *webrtc.RTPTransceiver, sfu.DownTrackState) {}
func (p *Participant) UncacheDownTrack(*webrtc.RTPTransceiver)                                    {}
func (p *Participant) GetCachedDownTrack(livekit.TrackID) (*webrtc.RTPTransceiver, sfu.DownTrackState) {
	return nil, sfu.DownTrackState{}
}

func (p *Participant) SetICEConfig(*livekit.ICEConfig)                                     {}
func (p *Participant) GetICEConfig() *livekit.ICEConfig                                    { return nil }
func (p *Participant) OnICEConfigChanged(func(types.LocalParticipant, *livekit.ICEConfig)) {}

// UpdateSubscribedQuality is dynacast's signal toward a publisher. v1 ignores it (all
// relayed layers flow); the v2 hook is EdgeEngine.SetLayerDemand per layer — see TODO.
func (p *Participant) UpdateSubscribedQuality(livekit.NodeID, livekit.TrackID, []types.SubscribedCodecQuality) error {
	return nil
}
func (p *Participant) UpdateSubscribedAudioCodecs(livekit.NodeID, livekit.TrackID, []*livekit.SubscribedAudioCodec) error {
	return nil
}
func (p *Participant) UpdateMediaLoss(livekit.NodeID, livekit.TrackID, uint32) error { return nil }

// ---- downstream bandwidth management: no subscriber side ---------------------------------------

func (p *Participant) SetSubscriberAllowPause(bool)       {}
func (p *Participant) SetSubscriberChannelCapacity(int64) {}

func (p *Participant) GetPacer() pacer.Pacer { return nil }

func (p *Participant) GetDisableSenderReportPassThrough() bool { return false }

func (p *Participant) HandleMetrics(livekit.ParticipantID, *livekit.MetricsBatch) error { return nil }

func (p *Participant) HandleUpdateSubscriptions([]livekit.TrackID, []*livekit.ParticipantTracks, bool) {
}
func (p *Participant) HandleUpdateSubscriptionPermission(*livekit.SubscriptionPermission) error {
	return nil
}
func (p *Participant) HandleSyncState(*livekit.SyncState) error { return nil }
func (p *Participant) HandleSimulateScenario(*livekit.SimulateScenario) error {
	return errRelayParticipant
}
func (p *Participant) HandleLeaveRequest(types.ParticipantCloseReason) {}

func (p *Participant) HandlePublishDataTrackRequest(*livekit.PublishDataTrackRequest)           {}
func (p *Participant) HandleUnpublishDataTrackRequest(*livekit.UnpublishDataTrackRequest)       {}
func (p *Participant) HandleUpdateDataSubscription(*livekit.UpdateDataSubscription)             {}
func (p *Participant) HandleStoreDataBlobRequest(*livekit.StoreDataBlobRequest)                 {}
func (p *Participant) HandleGetDataBlobRequest(*livekit.GetDataBlobRequest)                     {}
func (p *Participant) ProcessGetDataBlobRequest(*livekit.GetDataBlobRequest, types.Participant) {}

func (p *Participant) HandleSignalMessage(proto.Message) error { return errRelayParticipant }

func (p *Participant) PerformRpc(req *livekit.PerformRpcRequest, resultCh chan string, errorCh chan error) {
	go func() { errorCh <- errRelayParticipant }()
}

func (p *Participant) GetDataTrackTransport() types.DataTrackTransport { return nil }

func (p *Participant) ClearParticipantListener() {
	// Listener lives in params; the room clears it on close. Nothing retained beyond it.
}

func (p *Participant) GetNextSubscribedDataTrackHandle() uint16 { return 0 }

func (p *Participant) GetAllDataBlob() []*livekit.DataBlob { return nil }
