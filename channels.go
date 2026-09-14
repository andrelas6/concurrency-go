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

	TaskScheduler(ctx, tasks, MAX_CONCURRENT)
}

func TaskScheduler(ctx context.Context, tasks []Task, maxConcurrent int) (taskResults []TaskResult) {
	taskCh := make(chan Task, len(tasks))
	defer close(taskCh)
	for _, t := range tasks {
		taskCh <- t
	}
	// out channel - returns the result unbuffered
	taskResultCh := make(chan TaskResult, len(tasks))
	defer close(taskResultCh)
	tokenBudgetCh := make(chan struct{}, maxConcurrent)
	defer close(tokenBudgetCh)
	var i int

	// task runner
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case task := <-taskCh:
				<-tokenBudgetCh
				i++
				fmt.Println("RECEIVED NEW TOKEN", i)
				taskResultCh <- task()
				tokenBudgetCh <- struct{}{}
			}
		}
	}()

	// read values
	for result := range taskResultCh {
		taskResults = append(taskResults, result)
	}

	return taskResults
}

// true -> success
// false -> failure
type TaskResult bool
type Task func() TaskResult

func getTasks() (tasks []Task) {
	for i := range 10 {
		task := func() TaskResult {
			fmt.Printf("Running task %d ...", i)
			return i%2 == 0
		}
		tasks = append(tasks, task)
	}

	return tasks
}
