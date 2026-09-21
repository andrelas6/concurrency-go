package maxconcurrentrequests

import (
	"sync"
)

const MAX_CONCURRENT = 2

type Result[T any] struct {
	Value T
}
type Task[T any] func() Result[T]

// write to channel, read to chea
// buffered channel or another channel no notify that this is done
func RunConcurrently[T any](tasks []Task[T], max_concurrent int) <-chan Result[T] {
	var wg sync.WaitGroup
	ch := make(chan Result[T])
	// blocks writers from sending data until a reader gets it out
	concurrentCounter := make(chan struct{}, max_concurrent)
	// add tokens
	for range MAX_CONCURRENT {
		concurrentCounter <- struct{}{}
	}

	for _, t := range tasks {
		// sending all of them, but need to send only if one of the two are done
		wg.Go(func() {
			// get the token
			<-concurrentCounter

			defer func() {
				// release the token because you are done
				concurrentCounter <- struct{}{}
			}()

			// put the result in there
			result := t()
			ch <- result
		})
	}

	go func() {
		wg.Wait()
		close(ch)
	}()

	return ch
}
