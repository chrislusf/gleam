package instruction

import (
	"io"
	"strconv"
	"sync"

	"github.com/chrislusf/gleam/pb"
	"github.com/chrislusf/gleam/util"
)

func init() {
	InstructionRunner.Register(func(m *pb.Instruction) Instruction {
		if m.GetUnion() != nil {
			return NewUnion(m.GetUnion().GetIsParallel())
		}
		return nil
	})
}

type Union struct {
	isParallel bool
}

func NewUnion(isParallel bool) *Union {
	return &Union{
		isParallel: isParallel,
	}
}

func (b *Union) Name(prefix string) string {
	return "Union: isParallel=" + strconv.FormatBool(b.isParallel)
}

func (b *Union) Function() func(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
	return func(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
		return DoUnion(readers, writers[0], b.isParallel, stats)
	}
}

func (b *Union) SerializeToCommand() *pb.Instruction {
	return &pb.Instruction{
		Union: &pb.Instruction_Union{
			IsParallel: b.isParallel,
		},
	}
}

func (b *Union) GetMemoryCostInMB(partitionSize int64) int64 {
	return 3
}

func DoUnion(readers []io.Reader, writer io.Writer, isParallel bool,
	stats *pb.InstructionStat) error {

	var mutex sync.Mutex
	var wg sync.WaitGroup
	wg.Add(len(readers))
	defer wg.Wait()

	procReader := func(reader io.Reader) {
		util.ProcessRow(reader, nil, func(row *util.Row) error {
			mutex.Lock()
			row.WriteTo(writer)
			stats.InputCounter++
			stats.OutputCounter++
			mutex.Unlock()
			return nil
		})
		wg.Done()
	}

	if isParallel {
		for _, reader := range readers {
			go procReader(reader)
		}
	} else {
		go func() {
			for _, reader := range readers {
				procReader(reader)
			}
		}()
	}

	return nil
}
