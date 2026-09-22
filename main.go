package main

import (
	cli "concurrency-samples/cli"
	utils "concurrency-samples/utils"
	worker "concurrency-samples/worker"
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// TODO 1: Add generic type to Task (the caller know what to expect back) DONE
// TODO 2: turn this into a server that receives requests, runs in the worker pool and then is cancellable by timeout
// TODO 2: next step is to add something useful of the sync package to this (maybe wg.Do or smth)
// TODO 3: Error handling with wrapping/unwrapping (errors.Is, errors.As)
// TODO 4: when to panic, recover and defer
// TODO 5:
func httpServer() {
	mux := http.NewServeMux()

	// GET
	mux.HandleFunc("GET /task/{s}", func(w http.ResponseWriter, r *http.Request) {
		s := r.PathValue("s")
		if s == "" {
			http.Error(w, "missing ?s=", http.StatusBadRequest)
			return
		}
		fmt.Fprintf(w, "got %q\n", s)
	})

	log.Println("listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", mux))
}

func main() {
	httpServer()
	cliArgs := cli.GetCliArgs()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, cliArgs.ProgramTimeout)
	defer cancel()
	tasksStream := utils.GetTasksAsync(ctx)

	start := time.Now()
	// tasks := getTasks(10)
	workerPool := worker.NewWorker[string](cliArgs.Workers)

	go func() {
		// unblock main so that it starts reading
		for t := range tasksStream {
			workerPool.Run(t, ctx)
		}

	}()

	// resultStream := worker.RunTasks(tasks, 10, ctx)

	// for r := range workerPool.ResultStream {
	// 	fmt.Printf("%+v\n", r)
	// }

	for {
		select {
		case r := <-workerPool.ResultStream:
			fmt.Printf("Received %v\n", r)
			elapsed := time.Since(start)
			fmt.Printf("TIME ELAPSED %v\n", elapsed)
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				fmt.Println("timed out")
			} else {
				fmt.Println("INTERRUPTED")
			}

			elapsed := time.Since(start)
			fmt.Printf("TIME ELAPSED %v\n", elapsed)

			return
		}
	}

}
