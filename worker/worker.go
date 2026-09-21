package worker

import (
	"context"
	"fmt"
)

type Task[T any] func() T
type Result[T any] struct {
	Val   T
	Error error
}

func run[T any](w Task[T]) Result[T] {
	r := w()
	return Result[T]{Val: r, Error: nil}
}

// func (w *WorkerPool[T]) RunTask(task Task[T], ctx context.Context) {

// }

func RunTasks[T any](tasks []Task[T], maxConcurrently int, ctx context.Context) <-chan Result[T] {
	workerPool := NewWorker[T](maxConcurrently)
	ch := make(chan Result[T])
	for _, t := range tasks {
		// need to return this somehow
		workerPool.Run(t, ctx)
	}

	go func() {
		defer close(ch)
		for range len(tasks) {
			select {
			case r := <-workerPool.ResultStream:
				select {
				case ch <- r:
				case <-ctx.Done():
					fmt.Println("cancelling run tasks 1")
					return
				}
			case <-ctx.Done():
				fmt.Println("cancelling run tasks 2")
				return
			}
		}
	}()

	return ch
}

type WorkerPool[T any] struct {
	tokenStream  chan struct{}
	ResultStream chan Result[T]
}

func NewWorker[T any](size int) *WorkerPool[T] {
	tokenStream := make(chan struct{}, size)
	resultStream := make(chan Result[T])

	for range size {
		tokenStream <- struct{}{}
	}

	return &WorkerPool[T]{
		tokenStream:  tokenStream,
		ResultStream: resultStream,
	}
}

func (w *WorkerPool[T]) Run(t Task[T], ctx context.Context) {
	go func() {
		select {
		case <-w.tokenStream:
			defer func() {
				w.tokenStream <- struct{}{}
			}()
			select {
			case w.ResultStream <- run(t):
			case <-ctx.Done():
				fmt.Println("WorkerPool.run 1 done")
				return
			}
		case <-ctx.Done():
			fmt.Println("WorkerPool.run 2 done")
			return
		}
	}()
}
