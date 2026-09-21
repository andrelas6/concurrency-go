package utils

import (
	"concurrency-samples/worker"
	"context"
	"fmt"
	"math/rand"
	"time"
)

func GetTasks(count int) []worker.Task[string] {
	tasks := make([]worker.Task[string], 0)
	for i := range count {
		tasks = append(tasks, func() string {
			time.Sleep(time.Duration(rand.Intn(1e3)) * time.Millisecond)
			return fmt.Sprintf("%v", i)
		})
	}

	return tasks
}

// TODO 1: Add generic type to Task (the caller know what to expect back) DONE
// TODO 2: turn this into a server that receives requests, runs in the worker pool and then is cancellable by timeout
// TODO 2: next step is to add something useful of the sync package to this (maybe wg.Do or smth)
// TODO 3: Error handling with wrapping/unwrapping (errors.Is, errors.As)
// TODO 4: when to panic, recover and defer
// TODO 5:
func GetTasksAsync(ctx context.Context) <-chan worker.Task[string] {
	tasks := GetTasks(20)
	ch := make(chan worker.Task[string])
	go func() {
		defer close(ch)
		for _, t := range tasks {
			time.Sleep(time.Duration(rand.Intn(1e3)) * time.Millisecond)
			select {
			case ch <- t:
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch
}
