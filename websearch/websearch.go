package websearch

import (
	"fmt"
	"math/rand"
	"time"
)

type Result func(term string) string

func GetWebSearch(t string) Result {
	return func(term string) string {
		time.Sleep(time.Duration(rand.Intn(1e3)) * time.Millisecond)
		return fmt.Sprintf("Researched: %s", term)
	}
}

var (
	Text  = GetWebSearch("Text")
	Image = GetWebSearch("Image")
	Video = GetWebSearch("Video")
)

func SearchTerm(term string) string {
	// running sequentially
	// text := Text(term)
	// image := Image(term)
	// video := Video(term)

	// run in parallel
	ch := make(chan [2]string)
	go func() {
		ch <- [2]string{"text", Text(term)}
	}()
	go func() {
		ch <- [2]string{"image", Image(term)}
	}()
	go func() {
		ch <- [2]string{"video", Video(term)}
	}()

	results := struct {
		t string
		i string
		v string
	}{}

	collectResponses := func() string {

		multiline := fmt.Sprintf(`
		#### Results ####
			Text: %s,
			Image: %s,
			Video: %s
		`, results.t, results.i, results.v)
		return multiline
	}
	timeout := time.After(600 * time.Millisecond)
	for range 3 {
		select {
		case v := <-ch:
			switch v[0] {
			case "text":
				results.t = v[1]
			case "image":
				results.i = v[1]
			case "video":
				results.v = v[1]
			}
		case <-timeout:
			fmt.Println("too slow")
			return collectResponses()
		}

	}

	return collectResponses()
}

// func main() {
// 	term := "pokemon"

// 	start := time.Now()
// 	results := SearchTerm(term)
// 	elapsed := time.Since(start)

// 	fmt.Println(results)
// 	fmt.Println(elapsed)
// }

// func main() {
// 	quit := make(chan string)
// 	c1 := boring("Pikachu", quit)
// 	c2 := boring("Charmander", quit)
// 	c := fanIn(c1, c2)

// 	timeout := time.After(1 * time.Second)

// 	for {
// 		select {
// 		case s := <-c:
// 			fmt.Printf("You say: %q\n", s)
// 		case <-timeout:
// 			quit <- "Bye!"
// 		case <-quit:
// 			fmt.Println("All chatters are done!")
// 			return
// 			// case <-time.After(1 * time.Second):
// 			// 	fmt.Printf("You are too slow!")
// 			// 	return
// 		}
// 	}
// 	// for range 10 {
// 	// 	// why %q
// 	// 	fmt.Printf("You say: %q\n", <-c)
// 	// }

// 	fmt.Println("You're boring; I'm leaving")
// }
