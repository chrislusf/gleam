package instruction

import (
	"io"
	"log"

	"github.com/chrislusf/gleam/pb"
	"github.com/chrislusf/gleam/util"
)

func init() {
	InstructionRunner.Register(func(m *pb.Instruction) Instruction {
		if m.GetMergeTo() != nil {
			return NewMergeTo()
		}
		return nil
	})
}

type MergeTo struct{}

func NewMergeTo() *MergeTo {
	return &MergeTo{}
}

func (b *MergeTo) Name(prefix string) string {
	return prefix + ".MergeTo"
}

func (b *MergeTo) Function() func(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
	return func(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
		return DoMergeTo(readers, writers[0], stats)
	}
}

func (b *MergeTo) SerializeToCommand() *pb.Instruction {
	return &pb.Instruction{
		MergeTo: &pb.Instruction_MergeTo{},
	}
}

func (b *MergeTo) GetMemoryCostInMB(partitionSize int64) int64 {
	return 3
}

func DoMergeTo(readers []io.Reader, writer io.Writer, stats *pb.InstructionStat) error {
	// enqueue one item to the pq from each channel
	for _, reader := range readers {
		x, err := util.ReadMessage(reader)
		for err == nil {
			stats.InputCounter++
			if err := util.WriteMessage(writer, x); err != nil {
				return err
			}
			stats.OutputCounter++
			x, err = util.ReadMessage(reader)
		}
		if err != io.EOF {
			log.Printf("DoMergeTo failed start :%v", err)
			return err
		}
	}
	return nil
}
