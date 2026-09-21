package taskorchestrator

import (
	workerpool "concurrency-samples/task_orchestrator/workerpool"
)

type Task func() any

type Orchestrate interface {
	Enqueue(Task)
	ResultStream() <-chan any
}

type Orchestrator struct {
	resultStream chan any
	taskStream   chan Task
	doneStream   chan struct{}
	workerPool   *workerpool.WorkerPool
	queue        []Task
}

// Done implements [Orchestrate].
func (o *Orchestrator) Done() {
	close(o.doneStream)
}

// Run implements [Orchestrate].
func (o *Orchestrator) run() {
	go func() {
		for {
			select {
			case t, ok := <-o.taskStream:
				if !ok {
					return
				}
				o.workerPool.ScheduleTask(t)
			case result := <-o.workerPool.TaskResultStream:
				o.resultStream <- result
			case <-o.doneStream:
				o.workerPool.Done()
				return
			}
		}
	}()
}

// GetResultsStream implements [Orchestrate].
func (o *Orchestrator) ResultStream() <-chan any {
	return o.resultStream
}

// RunTask implements [Orchestrate].
func (o *Orchestrator) Enqueue(t Task) {
	o.queue = append(o.queue, t)
	go func() {
		o.taskStream <- t
	}()
}

var _ Orchestrate = (*Orchestrator)(nil)

func Init() *Orchestrator {
	inputStream := make(chan Task)
	outputStream := make(chan any)
	doneStream := make(chan struct{})

	workerPool := workerpool.New(2)

	o := &Orchestrator{
		resultStream: outputStream,
		taskStream:   inputStream,
		doneStream:   doneStream,
		workerPool:   workerPool,
		queue:        make([]Task, 0),
	}

	o.run()

	return o
}
