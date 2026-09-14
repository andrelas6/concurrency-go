package main

import (
	"context"
	"fmt"
	"sync"
	"time"
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

// ch <- chan int (read)
// ch chan <- int (write)
func RunNextTask(task Task, out chan<- TaskResult, wg *sync.WaitGroup) {
	result := task()
	out <- result
	if wg != nil {
		wg.Done()
	}
}

func TaskScheduler(ctx context.Context, tasks []Task, maxConcurrent int) (taskResults []TaskResult) {
	out := make(chan TaskResult, len(tasks))
	sem := make(chan struct{}, maxConcurrent)

	var wg sync.WaitGroup

loop:
	for _, task := range tasks {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			break loop
		}
		wg.Add(1)
		go func(t Task) {
			result := t()
			fmt.Printf("Running task %d\n", result.Id)
			out <- t()
			<-sem
			wg.Done()
		}(task)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	for o := range out {
		taskResults = append(taskResults, o)
	}

	return taskResults
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
			time.Sleep(2 * time.Second)
			return TaskResult{
				Result: i%2 == 0,
				Id:     i,
			}
		}
		tasks = append(tasks, task)
	}

	return tasks
}
