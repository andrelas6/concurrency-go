package cli

import (
	"flag"
	"time"
)

type CliArgs struct {
	Workers        int
	ProgramTimeout time.Duration
}

func GetCliArgs() CliArgs {
	workers := flag.Int("workers", 2, "numbers of workes in the pool")
	programTimeout := flag.Duration("timeout", 10*time.Second, "program timeout")

	flag.Parse()

	return CliArgs{
		Workers:        *workers,
		ProgramTimeout: *programTimeout,
	}
}
