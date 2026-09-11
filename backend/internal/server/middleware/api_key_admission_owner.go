package middleware

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"strconv"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/pkg/ip"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

func nextWithAPIKeyAdmissionOwner(c *gin.Context, apiKeyService *service.APIKeyService, credential string, trustedClientIP string, key *service.APIKey, googleStyle bool) {
	var writer *apiKeyAdmissionWriter
	if key.ConcurrencyLimit > 0 {
		// Install the queue revalidator before the admission owner so both the
		// request context and the owner's WithoutCancel-derived control context
		// carry it: a waiting request stops immediately when the key is
		// disabled/deleted/expired or its group, IP ACL and limit change.
		base := service.WithAPIKeyQueueAuthRevalidator(
			c.Request.Context(),
			newAPIKeyQueueAuthRevalidator(apiKeyService, credential, trustedClientIP, key),
		)
		ctx, cancel := service.WithAPIKeyAdmissionOwner(base)
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

// newAPIKeyQueueAuthRevalidator re-reads the authenticated key through the
// existing auth cache (L1, then L2, with at most one database read after an
// invalidation) and re-applies the same gates as initial authentication,
// including the captured trusted client IP and group permission premises. It
// closes over the credential but never logs it. A definite rejection stops the
// queue wait with the same application error the middleware would return; any
// other failure is classified by the queue as a service error.
func newAPIKeyQueueAuthRevalidator(apiKeyService *service.APIKeyService, credential string, trustedClientIP string, initial *service.APIKey) service.APIKeyQueueAuthRevalidator {
	return func(ctx context.Context) (int, error) {
		if apiKeyService == nil || credential == "" || initial == nil {
			return 0, service.NewAPIKeyQueueAuthRejected(infraerrors.ServiceUnavailable("API_KEY_AUTH_UNAVAILABLE", "API key authentication is temporarily unavailable"))
		}
		latest, err := apiKeyService.GetByKey(ctx, credential)
		if err != nil {
			if errors.Is(err, service.ErrAPIKeyNotFound) {
				return 0, service.NewAPIKeyQueueAuthRejected(infraerrors.Unauthorized("INVALID_API_KEY", "Invalid API key"))
			}
			return 0, service.NewAPIKeyQueueAuthRejected(infraerrors.ServiceUnavailable("API_KEY_AUTH_UNAVAILABLE", "API key authentication is temporarily unavailable"))
		}
		if latest == nil || latest.ID != initial.ID {
			return 0, service.NewAPIKeyQueueAuthRejected(infraerrors.Unauthorized("INVALID_API_KEY", "Invalid API key"))
		}
		// disabled / deleted / unknown status are unconditional rejections; an
		// expired or exhausted key is rejected in the queue as well because the
		// wait must not revive a dead authorization.
		if !latest.IsActive() &&
			latest.Status != service.StatusAPIKeyExpired &&
			latest.Status != service.StatusAPIKeyQuotaExhausted {
			return 0, service.NewAPIKeyQueueAuthRejected(infraerrors.Unauthorized("API_KEY_DISABLED", "API key is disabled"))
		}
		if latest.Status == service.StatusAPIKeyExpired || latest.IsExpired() {
			return 0, service.NewAPIKeyQueueAuthRejected(service.ErrAPIKeyExpired)
		}
		if latest.Status == service.StatusAPIKeyQuotaExhausted || latest.IsQuotaExhausted() {
			return 0, service.NewAPIKeyQueueAuthRejected(service.ErrAPIKeyQuotaExhausted)
		}
		if latest.User == nil || !latest.User.IsActive() {
			return 0, service.NewAPIKeyQueueAuthRejected(infraerrors.Unauthorized("USER_INACTIVE", "User account is not active"))
		}
		if code, message, ok := validateAPIKeyGroupAvailable(latest); !ok {
			return 0, service.NewAPIKeyQueueAuthRejected(infraerrors.Forbidden(code, message))
		}
		if !validateAPIKeyGroupAllowed(latest) {
			return 0, service.NewAPIKeyQueueAuthRejected(infraerrors.Forbidden("GROUP_NOT_ALLOWED", "API Key 所属专属分组不再允许当前用户使用"))
		}
		// IP ACL: re-run the existing rule check with the trusted IP captured at
		// authentication time. Raw request headers must not be re-parsed here.
		if len(latest.IPWhitelist) > 0 || len(latest.IPBlacklist) > 0 {
			allowed, _ := ip.CheckIPRestrictionWithCompiledRules(trustedClientIP, latest.CompiledIPWhitelist, latest.CompiledIPBlacklist)
			if !allowed {
				return 0, service.NewAPIKeyQueueAuthRejected(infraerrors.Forbidden("ACCESS_DENIED", "Access denied"))
			}
		}
		if apiKeyQueuePermissionPremisesChanged(initial, latest) {
			// The route authorization captured at authentication time is stale.
			// Abort instead of forwarding under the old group/platform rights.
			return 0, service.NewAPIKeyQueueAuthRejected(infraerrors.Forbidden("API_KEY_GROUP_CHANGED", "API key group changed while waiting; please retry"))
		}
		return latest.ConcurrencyLimit, nil
	}
}

// apiKeyQueuePermissionPremisesChanged treats any change to the captured group
// authorization as stale: platform/status, exclusive binding, model allowlist
// and routing, Live/messages/image permissions and client restrictions all gate
// handler routes. Pricing-only edits are included as a conservative superset;
// an aborted waiter re-authenticates and is re-routed with the fresh snapshot.
func apiKeyQueuePermissionPremisesChanged(initial, latest *service.APIKey) bool {
	if initial == nil || latest == nil {
		return true
	}
	if initial.GroupID == nil || latest.GroupID == nil {
		return initial.GroupID != latest.GroupID
	}
	if *initial.GroupID != *latest.GroupID {
		return true
	}
	return !reflect.DeepEqual(initial.Group, latest.Group)
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
