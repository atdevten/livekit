package relay

import (
	"errors"
	"fmt"

	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"google.golang.org/protobuf/proto"

	meshrelay "github.com/atdevten/mesh-relay/relay"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"

	"github.com/livekit/livekit-server/pkg/sfu"
	"github.com/livekit/livekit-server/pkg/sfu/buffer"
)

var errTrackClosed = errors.New("relay track closed")

// maxRelayLayers mirrors mesh-relay's layer bound (low/mid/high).
const maxRelayLayers = 3

// SyntheticReceiver presents a relayed remote track as a local publication. It embeds
// sfu.ReceiverBase — the same engine behind WebRTCReceiver — so downtrack fan-out,
// pull-based retransmission (ReadRTP), layer bitrates, and RTCP SR propagation to
// subscribers are all upstream code. This type only:
//
//   - constructs the base from the relayed TrackInfo (the metadata SDP would have
//     negotiated — mesh-relay pain point B1), and
//   - feeds relayed RTP into per-layer buffer.Buffers (buffers do jitter/keyframe/stats
//     exactly as for a local publisher).
//
// Local subscribers attach ordinary DownTracks via AddDownTrack; they cannot tell the
// publisher is remote.
type SyntheticReceiver struct {
	*sfu.ReceiverBase

	logger logger.Logger
	info   meshrelay.TrackInfo
	buffs  [maxRelayLayers]*buffer.Buffer
	kfr    chan uint8
	codec  webrtc.RTPCodecParameters
}

var _ sfu.TrackReceiver = (*SyntheticReceiver)(nil)

// NewSyntheticReceiver builds the local publication for one relayed track.
func NewSyntheticReceiver(info meshrelay.TrackInfo, log logger.Logger) (*SyntheticReceiver, error) {
	if info.CodecMime == "" || info.ClockRate == 0 {
		return nil, fmt.Errorf("relayed track %s missing codec params (B1): mime=%q clock=%d",
			info.TrackID, info.CodecMime, info.ClockRate)
	}

	kind := webrtc.RTPCodecTypeAudio
	lkType := livekit.TrackType_AUDIO
	if info.Kind == meshrelay.KindVideo {
		kind = webrtc.RTPCodecTypeVideo
		lkType = livekit.TrackType_VIDEO
	}

	codec := webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType:    info.CodecMime,
			ClockRate:   info.ClockRate,
			Channels:    info.Channels,
			SDPFmtpLine: info.Fmtp,
		},
		PayloadType: webrtc.PayloadType(info.PayloadType),
	}

	// The proto TrackInfo the rest of the server keys on. Sid carries the ORIGINAL track
	// id, so identity survives the relay (mesh-relay's headline win over the bridge bot).
	ti := &livekit.TrackInfo{
		Sid:  info.TrackID,
		Type: lkType,
		Name: info.TrackID,
	}
	layers := int(info.Layers)
	if layers < 1 {
		layers = 1
	}
	if layers > maxRelayLayers {
		layers = maxRelayLayers
	}
	for l := 0; l < layers; l++ {
		var ssrc uint32
		if l < len(info.SSRCs) {
			ssrc = info.SSRCs[l]
		}
		ti.Layers = append(ti.Layers, &livekit.VideoLayer{
			Quality: livekit.VideoQuality(l), //nolint:gosec // 0..2 by construction
			Ssrc:    ssrc,
		})
	}

	s := &SyntheticReceiver{
		logger: log,
		info:   info,
		kfr:    make(chan uint8, 4),
		codec:  codec,
	}
	s.ReceiverBase = sfu.NewReceiverBase(
		sfu.ReceiverBaseParams{
			TrackID:       livekit.TrackID(info.TrackID),
			StreamID:      info.SessionID,
			Kind:          kind,
			Codec:         codec,
			Logger:        log,
			IsSelfClosing: true,
		},
		ti,
		sfu.ReceiverCodecStateNormal,
	)

	// One buffer per relayed layer, fed by writeLayer. This mirrors WebRTCReceiver's
	// AddUpTrack (AddBuffer + feedback hook + StartBuffer), with the relay link playing
	// the role of the up track.
	for l := 0; l < layers; l++ {
		ssrc := uint32(0)
		if l < len(info.SSRCs) {
			ssrc = info.SSRCs[l]
		}
		buff := buffer.NewBuffer(ssrc, 500, 500)
		buff.SetLogger(log)
		if err := buff.Bind(
			webrtc.RTPParameters{Codecs: []webrtc.RTPCodecParameters{codec}},
			codec.RTPCodecCapability,
			0,
		); err != nil {
			return nil, fmt.Errorf("bind relay buffer layer %d: %w", l, err)
		}
		layer := l
		buff.OnRtcpFeedback(func(fb []rtcp.Packet) { s.onRTCPFeedback(layer, fb) })
		s.ReceiverBase.AddBuffer(buff, int32(l))
		s.ReceiverBase.StartBuffer(buff, int32(l))
		s.buffs[l] = buff
	}

	return s, nil
}

// onRTCPFeedback handles feedback the buffers/downstream generate for the "publisher".
// The publisher is on another server, so:
//   - PLI/FIR => cross-relay KeyFrameRequest (the origin translates it to a real PLI);
//   - NACK    => dropped: relay-link loss is repaired by mesh-relay's per-hop NACK before
//     packets ever reach these buffers, and subscriber-side loss is served from the
//     ReceiverBase retransmit path, not from the publisher.
func (s *SyntheticReceiver) onRTCPFeedback(layer int, pkts []rtcp.Packet) {
	for _, p := range pkts {
		switch p.(type) {
		case *rtcp.PictureLossIndication, *rtcp.FullIntraRequest:
			select {
			case s.kfr <- uint8(layer): //nolint:gosec // 0..2 by construction
			default:
			}
		}
	}
}

// sinkTrack adapts the SyntheticReceiver to mesh-relay's SinkTrack seam: the EdgeEngine
// writes repaired, reorder-tolerated packets here.
type sinkTrack struct{ r *SyntheticReceiver }

var (
	_ meshrelay.SinkTrack        = sinkTrack{}
	_ meshrelay.SenderReportSink = sinkTrack{}
)

func (t sinkTrack) WriteRTP(layer uint8, pkt *rtp.Packet) error {
	if int(layer) >= maxRelayLayers || t.r.buffs[layer] == nil {
		layer = 0
	}
	b := t.r.buffs[layer]
	if b == nil {
		return errTrackClosed
	}
	raw, err := pkt.Marshal()
	if err != nil {
		return err
	}
	_, err = b.Write(raw)
	return err
}

// WriteSenderReport decodes the relayed [layer][RTCPSenderReportState proto] payload and
// injects it into the matching buffer; ReceiverBase then propagates it to every local
// subscriber's downtrack (B4 lip-sync).
func (t sinkTrack) WriteSenderReport(payload []byte) error {
	if len(payload) < 2 {
		return errors.New("short sender report payload")
	}
	layer := payload[0]
	if int(layer) >= maxRelayLayers || t.r.buffs[layer] == nil {
		layer = 0
	}
	b := t.r.buffs[layer]
	if b == nil {
		return errTrackClosed
	}
	srData := &livekit.RTCPSenderReportState{}
	if err := proto.Unmarshal(payload[1:], srData); err != nil {
		return err
	}
	b.SetSenderReportData(srData)
	return nil
}

func (t sinkTrack) KeyFrameRequests() <-chan uint8 { return t.r.kfr }

func (t sinkTrack) Close() { t.r.ReceiverBase.Close("relay track ended", true) }

// SinkTrack exposes the mesh-relay seam view of this receiver.
func (s *SyntheticReceiver) SinkTrack() meshrelay.SinkTrack { return sinkTrack{r: s} }

// Info returns the relayed track metadata this receiver was built from.
func (s *SyntheticReceiver) Info() meshrelay.TrackInfo { return s.info }
