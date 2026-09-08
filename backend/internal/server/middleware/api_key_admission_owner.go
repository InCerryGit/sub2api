package middleware

import (
	"context"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"net/http"
)

func nextWithAPIKeyAdmissionOwner(c *gin.Context, key *service.APIKey) {
	if key.ConcurrencyLimit > 0 {
		ctx, cancel := service.WithAPIKeyAdmissionOwner(c.Request.Context())
		defer cancel()
		c.Request = c.Request.WithContext(ctx)
		c.Writer = &apiKeyAdmissionWriter{ResponseWriter: c.Writer, ctx: ctx}
	}
	c.Next()
}

// Before headers are committed a lease-loss cancellation is a service failure,
// not an upstream 502. An already-started stream is interrupted by its transport.
type apiKeyAdmissionWriter struct {
	gin.ResponseWriter
	ctx context.Context
}

func (w *apiKeyAdmissionWriter) WriteHeader(status int) {
	if !w.Written() && service.APIKeySlotLeaseLost(w.ctx) {
		status = http.StatusServiceUnavailable
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w *apiKeyAdmissionWriter) WriteHeaderNow() {
	if !w.Written() && service.APIKeySlotLeaseLost(w.ctx) {
		w.ResponseWriter.WriteHeader(http.StatusServiceUnavailable)
	}
	w.ResponseWriter.WriteHeaderNow()
}
