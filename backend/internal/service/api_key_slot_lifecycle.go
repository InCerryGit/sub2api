package service

import (
	"context"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
)

// APIKeySlotRefreshCache is optional so stats-only cache implementations remain
// compatible. The cache owns TTL units and chooses a cadence below its expiry.
type APIKeySlotRefreshCache interface {
	APIKeySlotRefreshInterval() time.Duration
	RefreshAPIKeySlot(context.Context, int64, string) (bool, error)
}

// One worker owns both renewal and removal. Cancellation joins that worker, so
// no refresh can race after removal, including when the request context ends.
func keepAPIKeySlot(ctx context.Context, cache APIKeyConcurrencyCache, apiKeyID int64, requestID string) func() {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer cancel()
		defer releaseAPIKeySlot(cache, apiKeyID, requestID)
		var ticks <-chan time.Time
		refresh, ok := cache.(APIKeySlotRefreshCache)
		if ok && refresh.APIKeySlotRefreshInterval() > 0 {
			ticker := time.NewTicker(refresh.APIKeySlotRefreshInterval())
			defer ticker.Stop()
			ticks = ticker.C
		}
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticks:
				opCtx, stop := context.WithTimeout(ctx, 2*time.Second)
				exists, err := refresh.RefreshAPIKeySlot(opCtx, apiKeyID, requestID)
				stop()
				if err != nil {
					logger.LegacyPrintf("service.concurrency", "Warning: failed to refresh api key slot for %d (req=%s): %v", apiKeyID, requestID, err)
				} else if !exists {
					// A manually removed or expired member must never be recreated.
					return
				}
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

func releaseAPIKeySlot(cache APIKeyConcurrencyCache, apiKeyID int64, requestID string) {
	// An ambiguous write or delete is safe to compensate with the same exact ID.
	// Bound the entire cleanup, including backoff, independently of the request.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		opCtx, stop := context.WithTimeout(ctx, time.Second)
		err = cache.ReleaseAPIKeySlot(opCtx, apiKeyID, requestID)
		stop()
		if err == nil {
			return
		}
		if attempt < 2 {
			timer := time.NewTimer(time.Duration(attempt+1) * 100 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
			case <-timer.C:
			}
		}
		if ctx.Err() != nil {
			break
		}
	}
	logger.LegacyPrintf("service.concurrency", "Warning: failed to release api key slot for %d (req=%s) after bounded retries: %v", apiKeyID, requestID, err)
}
