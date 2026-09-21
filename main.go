package main

import (
	utils "concurrency-samples/utils"
	worker "concurrency-samples/worker"
	"context"
	"fmt"
	"time"
)

// TODO 1: Add generic type to Task (the caller know what to expect back) DONE
// TODO 2: turn this into a server that receives requests, runs in the worker pool and then is cancellable by timeout
// TODO 2: next step is to add something useful of the sync package to this (maybe wg.Do or smth)
// TODO 3: Error handling with wrapping/unwrapping (errors.Is, errors.As)
// TODO 4: when to panic, recover and defer
// TODO 5:
func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10000*time.Millisecond)
	defer cancel()
	tasksStream := utils.GetTasksAsync(ctx)

	start := time.Now()
	// tasks := getTasks(10)
	workerPool := worker.NewWorker[string](5)

	for t := range tasksStream {
		workerPool.Run(t, ctx)
	}

	// resultStream := worker.RunTasks(tasks, 10, ctx)

	// for r := range workerPool.ResultStream {
	// 	fmt.Printf("%+v\n", r)
	// }

	for {
		select {
		case r := <-workerPool.ResultStream:
			fmt.Println(r)
		case <-ctx.Done():
			fmt.Println("timed out")
			elapsed := time.Since(start)
			fmt.Printf("TIME ELAPSED %v\n", elapsed)
			return
		}
	}

}
