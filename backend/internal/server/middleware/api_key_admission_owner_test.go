package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type missingAdmissionLeaseCache struct{ service.ConcurrencyCache }

func (*missingAdmissionLeaseCache) TrackAPIKeySlot(context.Context, int64, string) error { return nil }
func (*missingAdmissionLeaseCache) AcquireAPIKeySlot(context.Context, int64, int, string) (bool, error) {
	return true, nil
}
func (*missingAdmissionLeaseCache) ReleaseAPIKeySlot(context.Context, int64, string) error {
	return nil
}
func (*missingAdmissionLeaseCache) GetAPIKeyConcurrencyBatch(context.Context, []int64) (map[int64]int, error) {
	return nil, nil
}
func (*missingAdmissionLeaseCache) RefreshAPIKeySlot(context.Context, int64, string) (bool, error) {
	return false, nil
}
func (*missingAdmissionLeaseCache) APIKeySlotRefreshInterval() time.Duration { return time.Millisecond }
func (*missingAdmissionLeaseCache) APIKeySlotTTL() time.Duration             { return 3 * time.Second }

func TestAPIKeyAdmissionOwnerChangesUncommittedFailureTo503(t *testing.T) {
	router := gin.New()
	router.Use(func(c *gin.Context) { nextWithAPIKeyAdmissionOwner(c, &service.APIKey{ID: 1, ConcurrencyLimit: 1}) })
	router.GET("/forward", func(c *gin.Context) {
		lease, err := service.NewConcurrencyService(&missingAdmissionLeaseCache{}).AcquireAPIKeySlot(c.Request.Context(), 1, 1)
		require.NoError(t, err)
		defer lease.ReleaseFunc()
		select {
		case <-c.Request.Context().Done():
		case <-time.After(time.Second):
			t.Fatal("lease loss did not cancel request")
		}
		c.JSON(http.StatusBadGateway, gin.H{"error": "upstream interrupted"})
	})
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/forward", nil))
	require.Equal(t, http.StatusServiceUnavailable, response.Code)
}
