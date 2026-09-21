package workerpool

type WorkerPool struct {
	TaskResultStream chan any
	TaskQueueStream  chan func() any
	bucketStream     chan struct{}
	doneStream       chan struct{}
}

func (w *WorkerPool) Done() {
	w.doneStream <- struct{}{}
}

func (w *WorkerPool) ScheduleTask(t func() any) {
	go func() {
		w.TaskQueueStream <- t
	}()
}

func (w *WorkerPool) run() {
	go func() {
		for {
			select {
			case t := <-w.TaskQueueStream:
				<-w.bucketStream
				go func() {
					defer func() {
						w.bucketStream <- struct{}{}
					}()
					w.TaskResultStream <- t()
				}()

			case <-w.doneStream:
				close(w.TaskQueueStream)
				return
			}
		}
	}()
}

func New(size int) *WorkerPool {
	outputStream := make(chan any)
	bucketStream := make(chan struct{}, size)
	taskQueueStream := make(chan func() any)
	doneStream := make(chan struct{})

	for range size {
		bucketStream <- struct{}{}
	}

	w := &WorkerPool{
		TaskResultStream: outputStream,
		bucketStream:     bucketStream,
		TaskQueueStream:  taskQueueStream,
		doneStream:       doneStream,
	}

	w.run()

	return w
}
