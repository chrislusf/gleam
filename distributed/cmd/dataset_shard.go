package cmd

import (
	"fmt"
)

func (m *DatasetShard) Topic() string {
	return fmt.Sprintf("f%s-d%d-s%d", *m.FlowName, *m.DatasetId, *m.DatasetShardId)
}

func (m *DatasetShardLocation) Address() string {
	return fmt.Sprintf("%s:%d", *m.Host, *m.Port)
}
