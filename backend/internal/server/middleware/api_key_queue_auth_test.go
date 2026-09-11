//go:build unit

package middleware

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/Wei-Shaw/sub2api/internal/testutil"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func queueAuthTestKey(status string, limit int) *service.APIKey {
	group := &service.Group{
		ID:       42,
		Name:     "queue-group",
		Status:   service.StatusActive,
		Platform: service.PlatformOpenAI,
		Hydrated: true,
	}
	user := &service.User{ID: 7, Status: service.StatusActive, Concurrency: 3}
	apiKey := &service.APIKey{
		ID:               100,
		UserID:           user.ID,
		Key:              "queue-test-credential",
		Status:           status,
		ConcurrencyLimit: limit,
		User:             user,
		Group:            group,
	}
	apiKey.GroupID = &group.ID
	return apiKey
}

func TestNewAPIKeyQueueAuthRevalidatorRejectsDisabledKey(t *testing.T) {
	current := queueAuthTestKey(service.StatusActive, 1)
	repo := &stubApiKeyRepo{getByKey: func(context.Context, string) (*service.APIKey, error) {
		return current, nil
	}}
	svc := service.NewAPIKeyService(repo, nil, nil, nil, nil, nil, &config.Config{})
	revalidate := newAPIKeyQueueAuthRevalidator(svc, current.Key, "", current)

	current = queueAuthTestKey(service.StatusDisabled, 1)
	_, err := revalidate(context.Background())
	require.Error(t, err)
	var rejected *service.APIKeyQueueAuthRejectedError
	require.ErrorAs(t, err, &rejected)
	require.Equal(t, http.StatusUnauthorized, infraerrors.Code(err))
	require.Equal(t, "API_KEY_DISABLED", infraerrors.Reason(err))
}

func TestNewAPIKeyQueueAuthRevalidatorRejectsDeletedKey(t *testing.T) {
	current := queueAuthTestKey(service.StatusActive, 1)
	repo := &stubApiKeyRepo{getByKey: func(context.Context, string) (*service.APIKey, error) {
		return nil, service.ErrAPIKeyNotFound
	}}
	svc := service.NewAPIKeyService(repo, nil, nil, nil, nil, nil, &config.Config{})
	revalidate := newAPIKeyQueueAuthRevalidator(svc, current.Key, "", current)

	_, err := revalidate(context.Background())
	require.Error(t, err)
	require.Equal(t, http.StatusUnauthorized, infraerrors.Code(err))
	require.Equal(t, "INVALID_API_KEY", infraerrors.Reason(err))
}

func TestNewAPIKeyQueueAuthRevalidatorRejectsGroupChange(t *testing.T) {
	current := queueAuthTestKey(service.StatusActive, 1)
	repo := &stubApiKeyRepo{getByKey: func(context.Context, string) (*service.APIKey, error) {
		return current, nil
	}}
	svc := service.NewAPIKeyService(repo, nil, nil, nil, nil, nil, &config.Config{})
	revalidate := newAPIKeyQueueAuthRevalidator(svc, current.Key, "", current)

	moved := queueAuthTestKey(service.StatusActive, 1)
	other := int64(77)
	moved.GroupID = &other
	current = moved
	_, err := revalidate(context.Background())
	require.Error(t, err)
	require.Equal(t, http.StatusForbidden, infraerrors.Code(err))
	require.Equal(t, "API_KEY_GROUP_CHANGED", infraerrors.Reason(err))
}

func TestNewAPIKeyQueueAuthRevalidatorRejectsSameGroupPermissionChange(t *testing.T) {
	current := queueAuthTestKey(service.StatusActive, 1)
	current.Group.AllowLive = true
	repo := &stubApiKeyRepo{getByKey: func(context.Context, string) (*service.APIKey, error) {
		return current, nil
	}}
	svc := service.NewAPIKeyService(repo, nil, nil, nil, nil, nil, &config.Config{})
	revalidate := newAPIKeyQueueAuthRevalidator(svc, current.Key, "", current)

	// Same group ID, but the Live permission was revoked while waiting.
	revoked := queueAuthTestKey(service.StatusActive, 1)
	revoked.Group.AllowLive = false
	current = revoked
	_, err := revalidate(context.Background())
	require.Error(t, err)
	require.Equal(t, http.StatusForbidden, infraerrors.Code(err))
	require.Equal(t, "API_KEY_GROUP_CHANGED", infraerrors.Reason(err))
}

func TestNewAPIKeyQueueAuthRevalidatorRejectsIPBlacklistChange(t *testing.T) {
	current := queueAuthTestKey(service.StatusActive, 1)
	repo := &stubApiKeyRepo{getByKey: func(context.Context, string) (*service.APIKey, error) {
		return current, nil
	}}
	svc := service.NewAPIKeyService(repo, nil, nil, nil, nil, nil, &config.Config{})
	const trustedIP = "203.0.113.9"
	revalidate := newAPIKeyQueueAuthRevalidator(svc, current.Key, trustedIP, current)

	blocked := queueAuthTestKey(service.StatusActive, 1)
	blocked.IPBlacklist = []string{trustedIP}
	current = blocked
	_, err := revalidate(context.Background())
	require.Error(t, err)
	require.Equal(t, http.StatusForbidden, infraerrors.Code(err))
	require.Equal(t, "ACCESS_DENIED", infraerrors.Reason(err))
}

func TestNewAPIKeyQueueAuthRevalidatorReturnsFreshLimitAndCaches(t *testing.T) {
	key := queueAuthTestKey(service.StatusActive, 4)
	var repoCalls atomic.Int32
	repo := &stubApiKeyRepo{getByKey: func(context.Context, string) (*service.APIKey, error) {
		repoCalls.Add(1)
		return key, nil
	}}
	cfg := &config.Config{}
	cfg.APIKeyAuth.L1Size = 64
	cfg.APIKeyAuth.L1TTLSeconds = 60
	cfg.APIKeyAuth.L2TTLSeconds = 60
	authCache := &stubAPIKeyAuthCache{entries: map[string]*service.APIKeyAuthCacheEntry{}}
	svc := service.NewAPIKeyService(repo, nil, nil, nil, nil, authCache, cfg)
	revalidate := newAPIKeyQueueAuthRevalidator(svc, key.Key, "", key)

	for i := 0; i < 3; i++ {
		limit, err := revalidate(context.Background())
		require.NoError(t, err)
		require.Equal(t, 4, limit)
	}
	require.Equal(t, int32(1), repoCalls.Load(), "revalidation reuses the existing auth cache instead of polling the database")
}

// stubAPIKeyAuthCache is a minimal in-memory L2 auth cache.
type stubAPIKeyAuthCache struct {
	mu      sync.Mutex
	entries map[string]*service.APIKeyAuthCacheEntry
}

func (c *stubAPIKeyAuthCache) GetCreateAttemptCount(context.Context, int64) (int, error) {
	return 0, nil
}
func (c *stubAPIKeyAuthCache) IncrementCreateAttemptCount(context.Context, int64) error { return nil }
func (c *stubAPIKeyAuthCache) DeleteCreateAttemptCount(context.Context, int64) error    { return nil }
func (c *stubAPIKeyAuthCache) IncrementDailyUsage(context.Context, string) error        { return nil }
func (c *stubAPIKeyAuthCache) SetDailyUsageExpiry(context.Context, string, time.Duration) error {
	return nil
}
func (c *stubAPIKeyAuthCache) GetAuthCache(_ context.Context, key string) (*service.APIKeyAuthCacheEntry, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := c.entries[key]
	if entry == nil {
		return nil, errors.New("cache miss")
	}
	return entry, nil
}
func (c *stubAPIKeyAuthCache) SetAuthCache(_ context.Context, key string, entry *service.APIKeyAuthCacheEntry, _ time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[key] = entry
	return nil
}
func (c *stubAPIKeyAuthCache) DeleteAuthCache(_ context.Context, key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, key)
	return nil
}
func (c *stubAPIKeyAuthCache) PublishAuthCacheInvalidation(context.Context, string) error { return nil }
func (c *stubAPIKeyAuthCache) SubscribeAuthCacheInvalidation(context.Context, func(string)) error {
	return nil
}

// TestAPIKeyQueueMiddlewareInstallsRevalidator proves the installed callback is
// the actual path used by a queued request: the holder owns the only slot, the
// key is disabled while the request waits, and the wait ends with the same
// authentication error instead of forwarding.
func TestAPIKeyQueueMiddlewareInstallsRevalidator(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cache := testutil.NewRedisConcurrencyCache(t)
	queueCache, ok := cache.(service.APIKeySlotQueueCache)
	require.True(t, ok)

	key := queueAuthTestKey(service.StatusActive, 1)
	current := key
	var currentMu sync.Mutex
	loadCurrent := func() *service.APIKey {
		currentMu.Lock()
		defer currentMu.Unlock()
		return current
	}
	setCurrent := func(next *service.APIKey) {
		currentMu.Lock()
		current = next
		currentMu.Unlock()
	}
	var repoCalls atomic.Int32
	repo := &stubApiKeyRepo{getByKey: func(context.Context, string) (*service.APIKey, error) {
		repoCalls.Add(1)
		return loadCurrent(), nil
	}}
	cfg := &config.Config{RunMode: config.RunModeSimple}
	apiKeyService := service.NewAPIKeyService(repo, nil, nil, nil, nil, nil, cfg)

	concurrencyService := service.NewConcurrencyService(cache)
	concurrencyService.SetAPIKeyQueuePolicy(service.APIKeyQueuePolicy{MaxWaiting: 1, Timeout: 3 * time.Second})

	holderCtx, cancelHolder := service.WithAPIKeyAdmissionOwner(context.Background())
	defer cancelHolder()
	holder, err := concurrencyService.ReserveAPIKeySlotWithWait(holderCtx, key.ID, 1)
	require.NoError(t, err)
	require.NotNil(t, holder)
	defer holder.Release()

	var upstreamCalls atomic.Int32
	handlerResult := make(chan error, 1)
	router := gin.New()
	router.Use(gin.HandlerFunc(NewAPIKeyAuthMiddleware(apiKeyService, nil, cfg)))
	router.POST("/v1/messages", func(c *gin.Context) {
		reservation, reserveErr := concurrencyService.ReserveAPIKeySlotWithWait(c.Request.Context(), key.ID, 1)
		if reserveErr == nil && reservation != nil {
			upstreamCalls.Add(1)
			reservation.Release()
			c.Status(http.StatusOK)
			handlerResult <- nil
			return
		}
		c.Status(http.StatusUnauthorized)
		handlerResult <- reserveErr
	})

	request := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	request.Header.Set("x-api-key", key.Key)
	response := httptest.NewRecorder()
	serveDone := make(chan struct{})
	started := time.Now()
	go func() {
		router.ServeHTTP(response, request)
		close(serveDone)
	}()

	require.Eventually(t, func() bool {
		_, waiting, statsErr := queueCache.GetAPIKeyQueueStats(context.Background(), key.ID)
		return statsErr == nil && waiting == 1
	}, 2*time.Second, 5*time.Millisecond, "request must be waiting before the key changes")

	setCurrent(queueAuthTestKey(service.StatusDisabled, 1))
	select {
	case reserveErr := <-handlerResult:
		require.True(t, service.IsAPIKeyQueueErrorKind(reserveErr, service.APIKeyQueueErrorAuthRejected))
		require.Equal(t, http.StatusUnauthorized, infraerrors.Code(reserveErr))
		require.Equal(t, "API_KEY_DISABLED", infraerrors.Reason(reserveErr))
	case <-time.After(3 * time.Second):
		t.Fatal("queued request did not stop after the key was disabled")
	}
	require.Less(t, time.Since(started), 2*time.Second, "revalidation must end the wait promptly")
	<-serveDone
	require.Equal(t, http.StatusUnauthorized, response.Code)
	require.Zero(t, upstreamCalls.Load(), "a rejected request must not forward")
	require.Greater(t, repoCalls.Load(), int32(1), "queued wait re-read the key through the auth path")
	require.Eventually(t, func() bool {
		_, waiting, statsErr := queueCache.GetAPIKeyQueueStats(context.Background(), key.ID)
		return statsErr == nil && waiting == 0
	}, time.Second, 5*time.Millisecond, "rejected waiter cleans its ticket")
}

// TestAPIKeyQueueMiddlewareLimitZeroTracksOnly verifies the real middleware
// path when the key's limit becomes 0 while waiting: the queued attempt is
// closed first, then the existing unlimited tracking path owns the request.
func TestAPIKeyQueueMiddlewareLimitZeroTracksOnly(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cache := testutil.NewRedisConcurrencyCache(t)
	queueCache, ok := cache.(service.APIKeySlotQueueCache)
	require.True(t, ok)

	key := queueAuthTestKey(service.StatusActive, 1)
	current := key
	var currentMu sync.Mutex
	loadCurrent := func() *service.APIKey {
		currentMu.Lock()
		defer currentMu.Unlock()
		return current
	}
	setCurrent := func(next *service.APIKey) {
		currentMu.Lock()
		current = next
		currentMu.Unlock()
	}
	repo := &stubApiKeyRepo{getByKey: func(context.Context, string) (*service.APIKey, error) {
		return loadCurrent(), nil
	}}
	cfg := &config.Config{RunMode: config.RunModeSimple}
	apiKeyService := service.NewAPIKeyService(repo, nil, nil, nil, nil, nil, cfg)

	concurrencyService := service.NewConcurrencyService(cache)
	concurrencyService.SetAPIKeyQueuePolicy(service.APIKeyQueuePolicy{MaxWaiting: 1, Timeout: 3 * time.Second})

	holderCtx, cancelHolder := service.WithAPIKeyAdmissionOwner(context.Background())
	defer cancelHolder()
	holder, err := concurrencyService.ReserveAPIKeySlotWithWait(holderCtx, key.ID, 1)
	require.NoError(t, err)
	require.NotNil(t, holder)
	defer holder.Release()

	tracked := make(chan *service.APIKeySlotReservation, 1)
	handlerResult := make(chan error, 1)
	router := gin.New()
	router.Use(gin.HandlerFunc(NewAPIKeyAuthMiddleware(apiKeyService, nil, cfg)))
	router.POST("/v1/messages", func(c *gin.Context) {
		reservation, reserveErr := concurrencyService.ReserveAPIKeySlotWithWait(c.Request.Context(), key.ID, 1)
		if reserveErr != nil {
			handlerResult <- reserveErr
			c.Status(http.StatusInternalServerError)
			return
		}
		tracked <- reservation
		handlerResult <- nil
		c.Status(http.StatusOK)
	})

	request := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	request.Header.Set("x-api-key", key.Key)
	response := httptest.NewRecorder()
	serveDone := make(chan struct{})
	go func() {
		router.ServeHTTP(response, request)
		close(serveDone)
	}()

	require.Eventually(t, func() bool {
		_, waiting, statsErr := queueCache.GetAPIKeyQueueStats(context.Background(), key.ID)
		return statsErr == nil && waiting == 1
	}, 2*time.Second, 5*time.Millisecond, "request must be waiting before the key changes")

	setCurrent(queueAuthTestKey(service.StatusActive, 0))
	select {
	case reserveErr := <-handlerResult:
		require.NoError(t, reserveErr, "limit 0 continues on the unlimited tracking path")
	case <-time.After(3 * time.Second):
		t.Fatal("queued request did not switch to tracking after the limit changed")
	}
	<-serveDone
	require.Equal(t, http.StatusOK, response.Code)
	reservation := <-tracked
	require.True(t, reservation.StatsOnly(), "limit 0 has no enforced Key capacity")
	require.NotEmpty(t, reservation.RequestID(), "stats handle keeps an exact transfer identity")
	_, waiting, statsErr := queueCache.GetAPIKeyQueueStats(context.Background(), key.ID)
	require.NoError(t, statsErr)
	require.Zero(t, waiting, "queue ticket is closed before the tracking path starts")

	holder.Release()
	reservation.Release()
	_, active, statsErr := queueCache.GetAPIKeyQueueStats(context.Background(), key.ID)
	require.NoError(t, statsErr)
	require.Zero(t, active, "tracking member is released by its owner")
}

func TestAPIKeyQueueAuthRevalidatorFallbackWithoutServiceOrCredential(t *testing.T) {
	revalidate := newAPIKeyQueueAuthRevalidator(nil, "", "", nil)
	_, err := revalidate(context.Background())
	require.Error(t, err)
	require.Equal(t, http.StatusServiceUnavailable, infraerrors.Code(err))

	revalidate = newAPIKeyQueueAuthRevalidator(nil, "cred", "", queueAuthTestKey(service.StatusActive, 1))
	_, err = revalidate(context.Background())
	require.Error(t, err)
	require.Equal(t, http.StatusServiceUnavailable, infraerrors.Code(err))
}
