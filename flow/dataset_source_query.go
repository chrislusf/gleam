package flow

import (
	"bytes"
	"encoding/gob"
	"log"

	"github.com/chrislusf/gleam/adapter"
	"github.com/chrislusf/gleam/instruction"
)

// Query use the connection information specified via connectionId
// and then run the query to fetch the data as input
func (fc *FlowContext) Query(connectionId string, query adapter.AdapterQuery) (ret *Dataset) {
	ci, hasConnection := adapter.ConnectionManager.GetConnectionInfo(connectionId)
	if !hasConnection {
		log.Fatalf("Failed to find connection by id: %v", connectionId)
	}

	a, found := ci.GetAdapter()
	if !found {
		log.Fatalf("Failed to find adapter %s for %v.", ci.AdapterName, connectionId)
	}
	splits, err := a.GetSplits(connectionId, query)
	if err != nil {
		log.Fatalf("Failed to split query for connection %v, %v: %v", connectionId, query, err)
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
		ci.AdapterName,
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
