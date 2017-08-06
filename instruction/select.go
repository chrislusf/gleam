package instruction

import (
	"io"
	"strconv"
	"strings"

	"github.com/chrislusf/gleam/pb"
	"github.com/chrislusf/gleam/util"
)

func init() {
	InstructionRunner.Register(func(m *pb.Instruction) Instruction {
		if m.GetSelect() != nil {
			return NewSelect(
				toInts(m.GetSelect().GetKeyIndexes()),
				toInts(m.GetSelect().GetValueIndexes()),
			)
		}
		return nil
	})
}

type Select struct {
	keyIndexes   []int
	valueIndexes []int
}

func NewSelect(keyIndexes, valueIndexes []int) *Select {
	return &Select{keyIndexes, valueIndexes}
}

func (b *Select) Name(prefix string) string {
	return "Select k:" + joinInts(b.keyIndexes, ",") + " v:" + joinInts(b.valueIndexes, ",")
}

func (b *Select) Function() func(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
	return func(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
		return DoSelect(readers[0], writers[0], b.keyIndexes, b.valueIndexes, stats)
	}
}

func (b *Select) SerializeToCommand() *pb.Instruction {
	return &pb.Instruction{
		Select: &pb.Instruction_Select{
			KeyIndexes:   getIndexes(b.keyIndexes),
			ValueIndexes: getIndexes(b.valueIndexes),
		},
	}
}

func (b *Select) GetMemoryCostInMB(partitionSize int64) int64 {
	return 3
}

// DoSelect projects the fields
func DoSelect(reader io.Reader, writer io.Writer, keyIndexes, valueIndexes []int, stats *pb.InstructionStat) error {

	return util.ProcessRow(reader, nil, func(row *util.Row) error {
		stats.InputCounter++

		var keys, values []interface{}
		kLen := len(row.K)
		for _, x := range keyIndexes {
			if x <= kLen {
				keys = append(keys, row.K[x-1])
			} else {
				keys = append(keys, row.V[x-1-kLen])
			}
		}
		for _, x := range valueIndexes {
			if x <= kLen {
				values = append(values, row.K[x-1])
			} else {
				values = append(values, row.V[x-1-kLen])
			}
		}
		row.K, row.V = keys, values

		row.WriteTo(writer)
		stats.OutputCounter++

		return nil
	})

}

func joinInts(a []int, sep string) string {
	b := make([]string, len(a))
	for i, v := range a {
		b[i] = strconv.Itoa(v)
	}

	return strings.Join(b, sep)
}
