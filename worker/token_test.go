package worker

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestTokenReturn checks that a cancelled task gives its slot back to the pool.
func TestTokenReturn(t *testing.T) {
	pool := NewWorker(3)
	ctx, cancel := context.WithCancel(context.Background())

	// 3 tasks that finish instantly. Nobody ever reads resultStream,
	// so all 3 end up parked on the send, holding their tokens.
	for range 3 {
		pool.run(func() any { return 1 }, ctx)
	}
	time.Sleep(50 * time.Millisecond)

	cancel()                          // they all bail out via ctx.Done()
	time.Sleep(50 * time.Millisecond) // let them return

	got := len(pool.tokenStream)
	fmt.Printf("tokens back in pool: %d/%d\n", got, cap(pool.tokenStream))
	if got != 3 {
		t.Errorf("pool permanently lost %d of 3 slots", 3-got)
	}
}
