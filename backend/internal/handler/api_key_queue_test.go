package handler

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"testing"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/service"
	coderws "github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

func TestConcurrencyErrorResponseAPIKeyQueue(t *testing.T) {
	full := &service.APIKeyQueueError{Kind: service.APIKeyQueueErrorFull}
	timeout := &service.APIKeyQueueError{Kind: service.APIKeyQueueErrorTimeout}
	unavailable := &service.APIKeyQueueError{Kind: service.APIKeyQueueErrorUnavailable, Cause: errors.New("redis down")}
	changed := &service.APIKeyQueueError{Kind: service.APIKeyQueueErrorPolicyChanged}

	status, errType, code, message := concurrencyErrorResponse(full, "API key")
	require.Equal(t, http.StatusTooManyRequests, status)
	require.Equal(t, "rate_limit_error", errType)
	require.Equal(t, apiKeyQueueFullCode, code)
	require.Contains(t, message, "queue is full")

	status, _, code, _ = concurrencyErrorResponse(timeout, "API key")
	require.Equal(t, http.StatusTooManyRequests, status)
	require.Equal(t, apiKeyQueueTimeoutCode, code)

	status, _, _, _ = concurrencyErrorResponse(unavailable, "API key")
	require.Equal(t, http.StatusServiceUnavailable, status, "Redis faults must not masquerade as 429")

	status, _, _, _ = concurrencyErrorResponse(changed, "API key")
	require.Equal(t, http.StatusServiceUnavailable, status)
}

func TestConcurrencyErrorResponseAPIKeyQueueAuthRejected(t *testing.T) {
	rejected := &service.APIKeyQueueError{
		Kind:  service.APIKeyQueueErrorAuthRejected,
		Cause: infraerrors.Unauthorized("API_KEY_DISABLED", "API key is disabled"),
	}
	status, errType, code, message := concurrencyErrorResponse(rejected, "API key")
	require.Equal(t, http.StatusUnauthorized, status)
	require.Equal(t, "authentication_error", errType)
	require.Equal(t, "API_KEY_DISABLED", code)
	require.Contains(t, message, "disabled")

	expired := &service.APIKeyQueueError{
		Kind:  service.APIKeyQueueErrorAuthRejected,
		Cause: infraerrors.Forbidden("API_KEY_EXPIRED", "API key 已过期"),
	}
	status, errType, code, _ = concurrencyErrorResponse(expired, "API key")
	require.Equal(t, http.StatusForbidden, status)
	require.Equal(t, "permission_error", errType)
	require.Equal(t, "API_KEY_EXPIRED", code)

	require.Equal(t, coderws.StatusPolicyViolation, openAIWSUserSlotAcquireError(rejected).StatusCode())
}

func TestOpenAIWSQueueErrorsUseTypedCloseCodes(t *testing.T) {
	require.Equal(t, coderws.StatusTryAgainLater,
		openAIWSUserSlotAcquireError(&service.APIKeyQueueError{Kind: service.APIKeyQueueErrorFull}).StatusCode())
	require.Equal(t, coderws.StatusTryAgainLater,
		openAIWSUserSlotAcquireError(&service.APIKeyQueueError{Kind: service.APIKeyQueueErrorTimeout}).StatusCode())
	require.Equal(t, coderws.StatusInternalError,
		openAIWSUserSlotAcquireError(&service.APIKeyQueueError{Kind: service.APIKeyQueueErrorUnavailable}).StatusCode())
}

func TestParseAPIKeyQueueStatsIDs(t *testing.T) {
	ids, err := parseAPIKeyQueueStatsIDs("3,1,3,2")
	require.NoError(t, err)
	require.Equal(t, []int64{3, 1, 2}, ids)

	ids, err = parseAPIKeyQueueStatsIDs("")
	require.NoError(t, err)
	require.Empty(t, ids)

	for _, raw := range []string{"0", "-1", "abc", "1.5", "1,,?"} {
		_, err = parseAPIKeyQueueStatsIDs(raw)
		require.Error(t, err, "raw=%q", raw)
	}

	parts := make([]string, 0, apiKeyQueueStatsMaxIDs+1)
	for i := 1; i <= apiKeyQueueStatsMaxIDs+1; i++ {
		parts = append(parts, strconv.Itoa(i))
	}
	_, err = parseAPIKeyQueueStatsIDs(strings.Join(parts, ","))
	require.ErrorContains(t, err, "too many ids")
}

func TestOpsClassifiesQueueLimitsAsBusinessLimited(t *testing.T) {
	for _, code := range []string{apiKeyQueueFullCode, apiKeyQueueTimeoutCode} {
		phase := classifyOpsPhase("rate_limit_error", "API key wait queue is full, please retry later", code)
		require.Equal(t, "request", phase)
		require.True(t, isOpsLocalBusinessLimitError(code, "API key wait queue is full, please retry later"))
	}

	// Redis failures stay out of the business-limited bucket.
	require.False(t, isOpsLocalBusinessLimitError("", "Service temporarily unavailable, please retry later"))
	require.Equal(t, "internal", classifyOpsPhase("api_error", "Service temporarily unavailable, please retry later", ""))
}
