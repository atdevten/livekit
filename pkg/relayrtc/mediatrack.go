package relayrtc

import (
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"

	"github.com/livekit/livekit-server/pkg/relay"
	"github.com/livekit/livekit-server/pkg/rtc"
	"github.com/livekit/livekit-server/pkg/rtc/types"
)

// MediaTrack presents a relayed track (a relay.SyntheticReceiver) as a room-subscribable
// published track. Nearly everything is the upstream MediaTrackReceiver — the same base
// the WebRTC MediaTrack wraps — constructed with IsRelayed=true (the flag threaded through
// the subscription path that OSS never sets) and the synthetic receiver in place of a
// WebRTC one. Downtrack fan-out, subscription bookkeeping, quality mapping: all upstream
// code, untouched.
type MediaTrack struct {
	*rtc.MediaTrackReceiver

	logger logger.Logger
}

var _ types.MediaTrack = (*MediaTrack)(nil)

type MediaTrackParams struct {
	// Receiver is the relayed track's synthetic receiver (already carrying the proto
	// TrackInfo built from the relayed codec params — mesh-relay B1 fields).
	Receiver          *relay.SyntheticReceiver
	Participant       *Participant
	ReceiverConfig    rtc.ReceiverConfig
	SubscriberConfig  rtc.DirectionConfig
	TelemetryListener types.ParticipantTelemetryListener
	Logger            logger.Logger
}

func NewMediaTrack(params MediaTrackParams) *MediaTrack {
	ti := params.Receiver.TrackInfo()

	t := &MediaTrack{logger: params.Logger}
	t.MediaTrackReceiver = rtc.NewMediaTrackReceiver(rtc.MediaTrackReceiverParams{
		MediaTrack:          t,
		IsRelayed:           true,
		ParticipantID:       params.Participant.ID,
		ParticipantIdentity: params.Participant.Identity(),
		ParticipantVersion:  0,
		ReceiverConfig:      params.ReceiverConfig,
		SubscriberConfig:    params.SubscriberConfig,
		TelemetryListener:   params.TelemetryListener,
		Logger:              params.Logger,
	}, ti)

	// Attach the synthetic receiver exactly where a WebRTCReceiver would sit; local
	// DownTracks attach to it via the untouched AddSubscriber path.
	t.MediaTrackReceiver.SetupReceiver(params.Receiver, 0, "")

	return t
}

// ToProto returns the track's current proto state (mirrors upstream MediaTrack.ToProto).
func (t *MediaTrack) ToProto() *livekit.TrackInfo { return t.TrackInfoClone() }

// Logger implements types.MediaTrack.
func (t *MediaTrack) Logger() logger.Logger { return t.logger }

// OnTrackSubscribed is invoked when a non-hidden subscriber attaches. The WebRTC track
// uses it for first-subscription telemetry; the relay track has nothing to do — layer
// demand toward the origin is the edge engine's job (v2: SetLayerDemand per subscriber).
func (t *MediaTrack) OnTrackSubscribed() {}
