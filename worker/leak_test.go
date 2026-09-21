package worker

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"
)

// TestLeak mimics what main.go does: start tasks, then walk away when ctx fires.
func TestLeak(t *testing.T) {
	before := runtime.NumGoroutine()

	// Consumer scope: identical shape to main().
	func() {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		tasks := make([]Task, 0, 10)
		for i := range 10 {
			tasks = append(tasks, func() any {
				time.Sleep(200 * time.Millisecond) // still running when ctx fires
				return i
			})
		}

		stream := RunTasks(tasks, 10, ctx)
		for range len(tasks) {
			select {
			case <-stream:
			case <-ctx.Done():
				return // <- main.go:38 does exactly this
			}
		}
	}()

	// Give every goroutine a generous chance to finish and exit.
	time.Sleep(1 * time.Second)

	after := runtime.NumGoroutine()
	fmt.Printf("\n=== goroutines: before=%d after=%d leaked=%d ===\n\n", before, after, after-before)

	if after > before {
		buf := make([]byte, 1<<16)
		n := runtime.Stack(buf, true)
		fmt.Println(string(buf[:n]))
		t.Errorf("leaked %d goroutines", after-before)
	}
}
