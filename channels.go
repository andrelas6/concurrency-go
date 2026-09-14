package main

import (
	"context"
	"fmt"
)

const MAX_CONCURRENT = 3

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// three tasks at a time
	tasks := getTasks()

	result := TaskScheduler(ctx, tasks, MAX_CONCURRENT)

	for _, r := range result {
		fmt.Printf("RESULTS: %d %v\n", r.Id, r.Result)
	}
}

func TaskScheduler(ctx context.Context, tasks []Task, maxConcurrent int) (taskResults []TaskResult) {
	taskCh := make(chan Task, len(tasks))
	for _, t := range tasks {
		taskCh <- t
	}
	close(taskCh)
	// out channel - returns the result unbuffered
	taskResultCh := make(chan TaskResult, len(tasks))
	defer close(taskResultCh)
	out := make(chan TaskResult, len(tasks))

	for _ = range maxConcurrent {
		go func() {
			task := <-taskCh
			taskResultCh <- task()
		}()
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case result, ok := <-taskResultCh:
				if !ok {
					return
				} else {
					out <- result
					select {
					case task, ok := <-taskCh:
						if !ok {
							return
						} else {
							go func() {
								taskResultCh <- task()
							}()
						}
					case <-ctx.Done():
						return
					}
				}

			}
		}
	}()

	for o := range out {
		taskResults = append(taskResults, o)
	}
	return
}

// true -> success
// false -> failure
type TaskResult struct {
	Id     int
	Result bool
}
type Task func() TaskResult

func getTasks() (tasks []Task) {
	for i := range 10 {
		task := func() TaskResult {
			fmt.Printf("Running task %d ...\n", i)
			return TaskResult{
				Result: i%2 == 0,
				Id:     i,
			}
		}
		tasks = append(tasks, task)
	}

	return tasks
}
