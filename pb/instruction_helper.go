package pb

import (
	"fmt"
)

func (m *DatasetShard) Name() string {
	return fmt.Sprintf("f%d-d%d-s%d", m.FlowHashCode, m.DatasetId, m.DatasetShardId)
}

func (m *DatasetShardLocation) Address() string {
	return fmt.Sprintf("%s:%d", m.Host, m.Port)
}

func (m *InstructionSet) InstructionNames() (stepNames []string) {
	for _, ins := range m.GetInstructions() {
		stepNames = append(stepNames, fmt.Sprintf("%d:%d", ins.StepId, ins.TaskId))
	}
	return
}

func (i *Instruction) SetInputLocations(locations []DataLocation) {
	for _, loc := range locations {
		i.InputShardLocations = append(i.InputShardLocations, &DatasetShardLocation{
			Name:   loc.Name,
			Host:   loc.Location.Server,
			Port:   int32(loc.Location.Port),
			OnDisk: loc.OnDisk,
		})
	}
}

func (i *Instruction) SetOutputLocations(locations []DataLocation) {
	for _, loc := range locations {
		i.OutputShardLocations = append(i.OutputShardLocations, &DatasetShardLocation{
			Name:   loc.Name,
			Host:   loc.Location.Server,
			Port:   int32(loc.Location.Port),
			OnDisk: loc.OnDisk,
		})
	}
}

func (i *Instruction) GetName() string {
	return fmt.Sprintf("%d:%d", i.StepId, i.TaskId)
}
