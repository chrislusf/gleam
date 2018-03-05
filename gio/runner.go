package gio

import (
	"context"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"

	"github.com/chrislusf/gleam/pb"
)

// Serve starts processing stdin and writes output to stdout
func (runner *gleamRunner) runMapperReducer() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if runner.Option.IsProfiling {
		cpuProfFile := fmt.Sprintf("mr_cpu_%d-s%d-t%d.pprof", runner.Option.HashCode,
			runner.Option.StepId, runner.Option.TaskId)
		pwd, _ := os.Getwd()
		println("saving pprof to", pwd+"/"+cpuProfFile)

		cf, err := os.Create(cpuProfFile)
		if err != nil {
			log.Fatalf("failed to create cpu profile file %s: %v", pwd+"/"+cpuProfFile, err)
		}
		pprof.StartCPUProfile(cf)
		defer pprof.StopCPUProfile()

		memProfFile := fmt.Sprintf("mr_mem_%d-s%d-t%d.pprof", runner.Option.HashCode,
			runner.Option.StepId, runner.Option.TaskId)

		mf, err := os.Create(memProfFile)
		println("saving pprof to", pwd+"/"+memProfFile)
		if err != nil {
			log.Fatalf("failed to create memory profile file %s: %v", pwd+"/"+memProfFile, err)
		}

		defer func() {
			runtime.GC()
			pprof.Lookup("heap").WriteTo(mf, 0)

		}()
	}

	stat.FlowHashCode = uint32(runner.Option.HashCode)
	stat.Stats = []*pb.InstructionStat{
		{
			StepId: int32(runner.Option.StepId),
			TaskId: int32(runner.Option.TaskId),
		},
	}

	if runner.Option.Mapper != "" {
		if fn, ok := mappers[runner.Option.Mapper]; ok {
			if err := runner.processMapper(ctx, fn); err != nil {
				log.Fatalf("Failed to execute mapper %v: %v", os.Args, err)
			}
			return
		}
		log.Fatalf("Failed to find mapper function for %v", runner.Option.Mapper)

	}

	if runner.Option.Reducer != "" {
		if runner.Option.KeyFields == "" {
			log.Fatalf("Also expecting values for -gleam.keyFields! Actual arguments: %v", os.Args)
		}
		if fn, ok := reducers[runner.Option.Reducer]; ok {
			keyPositions := strings.Split(runner.Option.KeyFields, ",")
			var keyIndexes []int
			for _, keyPosition := range keyPositions {
				keyIndex, keyIndexError := strconv.Atoi(keyPosition)
				if keyIndexError != nil {
					log.Fatalf("Failed to parse key index positions %v: %v", runner.Option.KeyFields, keyIndexError)
				}
				keyIndexes = append(keyIndexes, keyIndex)
			}

			if err := runner.processReducer(ctx, fn, keyIndexes); err != nil {
				log.Fatalf("Failed to execute reducer %v: %v", os.Args, err)
			}
			return
		}
		log.Fatalf("Failed to find reducer function for %v", runner.Option.Reducer)

	}

	log.Fatalf("Failed to find function to execute. Args: %v", os.Args)
}
