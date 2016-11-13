package flow

import (
	"bytes"
	"encoding/gob"
	"log"

	"github.com/chrislusf/gleam/adapter"
	"github.com/chrislusf/gleam/instruction"
)

// Query create or reuse the connection specified via connectionId
// and then run the query to fetch the data as input
func (fc *FlowContext) Query(connectionId string, query adapter.AdapterQuery) (ret *Dataset) {
	ci, hasConnection := adapter.ConnectionManager.GetConnectionInfo(connectionId)
	if !hasConnection {
		log.Printf("Failed to find connection by id: %v", connectionId)
		return nil
	}

	splits, err := ci.Adapter.GetSplits(connectionId, query)
	if err != nil {
		log.Printf("Failed to split query for connection %v, %v: %v", connectionId, query, err)
		return nil
	}

	var encoded [][]byte
	for _, split := range splits {
		d := encodeSplit(split)
		// println("adding encoded data:", len(d), string(d))
		encoded = append(encoded, d)
	}

	parallelCount := len(encoded)
	if query.GetParallelLimit() > 0 && parallelCount > query.GetParallelLimit() {
		parallelCount = query.GetParallelLimit()
	}

	data := fc.Bytes(encoded).RoundRobin(parallelCount)

	ret = fc.newNextDataset(parallelCount)
	step := fc.AddOneToOneStep(data, ret)
	step.SetInstruction(instruction.NewAdapterSplitReader(
		ci.Adapter.AdapterName(),
		connectionId,
	))

	return ret
}

func encodeSplit(split adapter.Split) []byte {
	var network bytes.Buffer
	enc := gob.NewEncoder(&network)
	interfaceEncode(enc, split)
	return network.Bytes()
}

// interfaceEncode encodes the interface value into the encoder.
func interfaceEncode(enc *gob.Encoder, p adapter.Split) {
	err := enc.Encode(&p)
	if err != nil {
		log.Fatal("encode:", err)
	}
}
