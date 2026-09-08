package middleware

import (
	"context"
	"net/http"
	"strconv"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

func nextWithAPIKeyAdmissionOwner(c *gin.Context, key *service.APIKey, googleStyle bool) {
	var writer *apiKeyAdmissionWriter
	if key.ConcurrencyLimit > 0 {
		ctx, cancel := service.WithAPIKeyAdmissionOwner(c.Request.Context())
		defer cancel()
		c.Request = c.Request.WithContext(ctx)
		writer = &apiKeyAdmissionWriter{ResponseWriter: c.Writer, ctx: ctx, googleStyle: googleStyle, head: c.Request.Method == http.MethodHead}
		c.Writer = writer
	}
	c.Next()
	// Gin finalizes its internal writer after middleware returns. Handle a blank
	// return here, while lease loss is distinguishable from normal owner cleanup.
	if writer != nil {
		writer.rejectIfLost()
	}
}

type apiKeyAdmissionWriter struct {
	gin.ResponseWriter
	ctx         context.Context
	googleStyle bool
	head        bool
	rejected    bool
	writeErr    error
}

// Once headers are committed, preserve streaming behavior. Before that point,
// replace the representation entirely; upstream bytes must never follow the 503.
func (w *apiKeyAdmissionWriter) rejectIfLost() bool {
	if w.rejected {
		return true
	}
	if w.Written() || !service.APIKeySlotLeaseLost(w.ctx) {
		return false
	}
	w.rejected = true
	body := `{"type":"error","error":{"type":"api_error","message":"API key concurrency lease lost; please retry later"}}`
	if w.googleStyle {
		body = `{"error":{"code":503,"status":"UNAVAILABLE","message":"API key concurrency lease lost; please retry later"}}`
	}
	headers := w.Header()
	for _, name := range []string{"Content-Encoding", "Content-Range", "Content-Disposition", "ETag", "Last-Modified", "Trailer", "Transfer-Encoding"} {
		headers.Del(name)
	}
	headers.Set("Content-Type", "application/json; charset=utf-8")
	headers.Set("Content-Length", strconv.Itoa(len(body)))
	headers.Set("Cache-Control", "no-store")
	w.ResponseWriter.WriteHeader(http.StatusServiceUnavailable)
	if w.head {
		w.ResponseWriter.WriteHeaderNow()
	} else {
		_, w.writeErr = w.ResponseWriter.WriteString(body)
	}
	return true
}

func (w *apiKeyAdmissionWriter) WriteHeader(status int) {
	if !w.rejectIfLost() {
		w.ResponseWriter.WriteHeader(status)
	}
}

func (w *apiKeyAdmissionWriter) WriteHeaderNow() {
	if !w.rejectIfLost() {
		w.ResponseWriter.WriteHeaderNow()
	}
}

func (w *apiKeyAdmissionWriter) Write(data []byte) (int, error) {
	if w.rejectIfLost() {
		if w.writeErr != nil {
			return 0, w.writeErr
		}
		return len(data), nil // Consume discarded upstream bytes without mixing bodies.
	}
	return w.ResponseWriter.Write(data)
}

func (w *apiKeyAdmissionWriter) WriteString(data string) (int, error) {
	if w.rejectIfLost() {
		if w.writeErr != nil {
			return 0, w.writeErr
		}
		return len(data), nil
	}
	return w.ResponseWriter.WriteString(data)
}

func (w *apiKeyAdmissionWriter) Flush() {
	w.rejectIfLost()
	w.ResponseWriter.Flush()
}
