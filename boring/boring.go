package boring

import (
	"fmt"
	"math/rand"
	"time"
)

// generator pattern
// function that returns a channel
func boring(msg string, quit chan string) <-chan string {
	c := make(chan string)

	go func() {
		for i := 0; ; i++ {
			select {
			case c <- fmt.Sprintf("%s %d", msg, i):
				time.Sleep(time.Duration(rand.Intn(1e3)) * time.Millisecond)

			case <-quit:
				fmt.Println("doing cleanup")
				quit <- fmt.Sprintf("Done %s\n", msg)
				return
			}
		}
	}()
	// go func() {
	// 	for i := 0; ; i++ {
	// 		c <- fmt.Sprintf("%s %d", msg, i)
	// 		// any time between 0 and 1 second
	// 		time.Sleep(time.Duration(rand.Intn(1e3)) * time.Millisecond)
	// 	}
	// }()

	return c
}

// go routines unblock the main thread
func fanIn(input1, input2 <-chan string) <-chan string {
	// put those values in a single channel
	ch := make(chan string)

	go func() {
		for {
			select {
			case s := <-input1:
				ch <- s
			case s := <-input2:
				ch <- s
			}
		}
	}()
	// go func() {
	// 	for {
	// 		ch <- <-input1
	// 	}
	// }()
	// go func() {
	// 	for {
	// 		ch <- <-input2
	// 	}
	// }()

	return ch
}
