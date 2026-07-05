// Package relay integrates the mesh-relay inter-server protocol
// (github.com/atdevten/mesh-relay/relay) into livekit-server.
//
// It implements the two seam adapters from mesh-relay's §4.4 design:
//
//   - RelayDownTrack (this file): a TrackSender registered on a local receiver's fan-out,
//     exporting the track to peer servers ("the relay is just another subscriber").
//   - SyntheticReceiver (receiver.go): a TrackReceiver built from a relayed TrackUpdate,
//     presenting the remote track to local subscribers ("just another publisher").
//
// Everything protocol-level (QUIC, FlatBuffers, NACK, subscription gating, SR forwarding)
// lives in the imported mesh-relay module and is reused unchanged. This package is the
// entire fork diff surface, by design (mesh-relay pain point B12): new files only, plus
// the wiring hooks documented in RELAY-FORK.md.
package relay

import (
	"sync/atomic"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"google.golang.org/protobuf/proto"

	meshrelay "github.com/atdevten/mesh-relay/relay"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"

	"github.com/livekit/livekit-server/pkg/sfu"
	"github.com/livekit/livekit-server/pkg/sfu/buffer"
)

// relayQueueDepth bounds the per-track packet queue between the SFU fan-out and the relay
// pump. The fan-out must never block on a slow relay link, so WriteRTP drops when full —
// the peer's per-hop NACK recovers what matters, and sustained overflow shows up in the
// relay link's own counters.
const relayQueueDepth = 512

type queuedPkt struct {
	raw   []byte // copied: ExtPacket buffers are reused by the SFU after WriteRTP returns
	layer uint8
}

// RelayDownTrack exports one locally published track to the relay. It satisfies both
// sides of the boundary:
//
//   - sfu.TrackSender: registered via receiver.AddDownTrack, it receives every forwarded
//     packet exactly like a subscriber's downtrack — the fan-out loop is untouched.
//   - meshrelay.SourceTrack (+ SenderReportSource): consumed by the relay OriginEngine,
//     which announces the track to peers and pumps these packets into QUIC datagrams.
type RelayDownTrack struct {
	receiver sfu.TrackReceiver
	logger   logger.Logger
	room     string                // room the track is published in; rides TrackUpdate.room
	peerID   livekit.ParticipantID // synthetic subscriber identity for this relay link

	pkts chan queuedPkt
	srs  chan []byte

	closed   atomic.Bool
	closedCh chan struct{}
}

// Compile-time checks: both contracts, one type.
var (
	_ sfu.TrackSender              = (*RelayDownTrack)(nil)
	_ meshrelay.SourceTrack        = (*RelayDownTrack)(nil)
	_ meshrelay.SenderReportSource = (*RelayDownTrack)(nil)
)

// NewRelayDownTrack builds the exporter for one receiver. peerID identifies the relay
// link this track is exported on (it plays the role of the subscriber's participant id
// inside the SFU's downtrack bookkeeping).
func NewRelayDownTrack(receiver sfu.TrackReceiver, room string, peerID livekit.ParticipantID, log logger.Logger) *RelayDownTrack {
	return &RelayDownTrack{
		receiver: receiver,
		logger:   log,
		room:     room,
		peerID:   peerID,
		pkts:     make(chan queuedPkt, relayQueueDepth),
		srs:      make(chan []byte, 8),
		closedCh: make(chan struct{}),
	}
}

// ---- sfu.TrackSender (called by the SFU fan-out) ----------------------------------------

// WriteRTP receives one forwarded packet from the receiver's fan-out. Non-blocking: the
// SFU's forward loop must never stall on the relay, so a full queue drops the packet
// (recovered by the peer's NACK if it was subscribed and mattered).
func (t *RelayDownTrack) WriteRTP(p *buffer.ExtPacket, layer int32) int32 {
	if t.closed.Load() || p == nil || len(p.RawPacket) == 0 {
		return 0
	}
	raw := make([]byte, len(p.RawPacket))
	copy(raw, p.RawPacket)
	if layer < 0 {
		layer = 0
	}
	select {
	case t.pkts <- queuedPkt{raw: raw, layer: uint8(layer)}:
		return int32(len(raw))
	default:
		return 0 // queue full: drop, never block the fan-out
	}
}

func (t *RelayDownTrack) HandleRTCPSenderReportData(
	_ webrtc.PayloadType,
	layer int32,
	srData *livekit.RTCPSenderReportState,
) error {
	if t.closed.Load() || srData == nil {
		return nil
	}
	b, err := proto.Marshal(srData)
	if err != nil {
		return err
	}
	if layer < 0 {
		layer = 0
	}
	// Wire payload = [layer byte][proto-marshalled RTCPSenderReportState]; the synthetic
	// receiver on the far side decodes and injects it into the matching layer's buffer.
	payload := append([]byte{byte(layer)}, b...)
	select {
	case t.srs <- payload:
	default: // SRs are periodic; dropping one is harmless
	}
	return nil
}

// Up-track change notifications: the relay exports whatever exists; peers learn about
// layer availability via TrackUpdate, and demand flows back as Subscribe masks. Nothing
// to react to per-notification yet.
func (t *RelayDownTrack) UpTrackLayersChange()                       {}
func (t *RelayDownTrack) UpTrackBitrateAvailabilityChange()          {}
func (t *RelayDownTrack) UpTrackMaxPublishedLayerChange(int32)       {}
func (t *RelayDownTrack) UpTrackMaxTemporalLayerSeenChange(int32)    {}
func (t *RelayDownTrack) UpTrackBitrateReport([]int32, sfu.Bitrates) {}
func (t *RelayDownTrack) Resync()                                    {}
func (t *RelayDownTrack) SetReceiver(r sfu.TrackReceiver)            { t.receiver = r }
func (t *RelayDownTrack) ReceiverRestart(r sfu.TrackReceiver)        { t.receiver = r }

func (t *RelayDownTrack) ID() string { return string(t.receiver.TrackID()) + "_relay" }

func (t *RelayDownTrack) SubscriberID() livekit.ParticipantID { return t.peerID }

func (t *RelayDownTrack) Close() {
	if t.closed.CompareAndSwap(false, true) {
		close(t.closedCh)
	}
}

func (t *RelayDownTrack) IsClosed() bool { return t.closed.Load() }

// ---- meshrelay.SourceTrack (consumed by the relay OriginEngine) -------------------------

// Info assembles the TrackUpdate metadata (mesh-relay pain point B1: every codec field
// required for the far side to build a decodable track).
func (t *RelayDownTrack) Info() meshrelay.TrackInfo {
	codec := t.receiver.Codec()
	ti := t.receiver.TrackInfo()

	kind := meshrelay.KindAudio
	if ti.GetType() == livekit.TrackType_VIDEO {
		kind = meshrelay.KindVideo
	}

	layers := uint8(1)
	var ssrcs []uint32
	if ti != nil && len(ti.Layers) > 0 {
		layers = uint8(len(ti.Layers))
		for _, l := range ti.Layers {
			ssrcs = append(ssrcs, l.Ssrc)
		}
	}
	if len(ssrcs) == 0 {
		ssrcs = []uint32{0} // synthetic receiver allocates its own; SSRC is informational here
	}

	return meshrelay.TrackInfo{
		SessionID:   streamIDOf(t.receiver, ti),
		Room:        t.room,
		TrackID:     string(t.receiver.TrackID()),
		Kind:        kind,
		CodecMime:   codec.MimeType,
		Fmtp:        codec.SDPFmtpLine,
		ClockRate:   codec.ClockRate,
		Channels:    codec.Channels,
		PayloadType: uint8(codec.PayloadType),
		Layers:      layers,
		SSRCs:       ssrcs,
	}
}

// ReadRTP blocks for the next exported packet (the OriginEngine's pump loop).
func (t *RelayDownTrack) ReadRTP() (*rtp.Packet, uint8, error) {
	select {
	case q := <-t.pkts:
		pkt := &rtp.Packet{}
		if err := pkt.Unmarshal(q.raw); err != nil {
			return nil, 0, err
		}
		return pkt, q.layer, nil
	case <-t.closedCh:
		return nil, 0, errTrackClosed
	}
}

// RequestKeyFrame translates a cross-relay keyframe request into a PLI toward the real
// local publisher (the B4/A1 analogue: without this, remote viewers freeze after loss).
func (t *RelayDownTrack) RequestKeyFrame(layer uint8) {
	t.receiver.SendPLI(int32(layer), true)
}

// SenderReports surfaces the publisher's RTCP SRs for cross-relay lip-sync (B4).
func (t *RelayDownTrack) SenderReports() <-chan []byte { return t.srs }

func streamIDOf(r sfu.TrackReceiver, ti *livekit.TrackInfo) string {
	if sid := r.StreamID(); sid != "" {
		return sid
	}
	if ti != nil {
		return ti.Sid
	}
	return string(r.TrackID())
}
