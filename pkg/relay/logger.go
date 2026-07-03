package relay

import (
	"go.uber.org/zap"

	"github.com/livekit/protocol/logger"
)

// zapFrom bridges livekit's logr-based logger to the *zap.Logger the mesh-relay engines
// expect. The engines log operational events only, so a plain production zap logger is
// acceptable when unwrapping fails; relay events still carry their own key/values.
func zapFrom(_ logger.Logger) *zap.Logger {
	if l, err := zap.NewProduction(); err == nil {
		return l
	}
	return zap.NewNop()
}
