package main

import (
	"flag"
	"fmt"
	"gocodebrc/solutions"
	"os"
	"runtime/pprof"
)

func main() {
	var cpuProfile = flag.String("cpuprofile", "", "write CPU profile to file")
	var filePath = flag.String("file", "", "1 billion rows")

	// parse flags data
	flag.Parse()

	if *filePath == "" {
		panic("Pass filepath to process")
	}

	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	// solutions.Slow()
	// solutions.Naive(*filePath)
	solutions.Naive2(*filePath)

}
