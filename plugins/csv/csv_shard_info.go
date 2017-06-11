package csv

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"

	"github.com/chrislusf/gleam/filesystem"
	"github.com/chrislusf/gleam/gio"
)

type CsvShardInfo struct {
	Config    map[string]string
	FileName  string
	HasHeader bool
}

var (
	MapperReadShard = gio.RegisterMapper(readShard)
)

func init() {
	gob.Register(CsvShardInfo{})
}

func readShard(row []interface{}) error {
	encodedShardInfo := row[0].([]byte)
	return decodeShardInfo(encodedShardInfo).ReadSplit()
}

func (ds *CsvShardInfo) ReadSplit() error {

	// println("opening file", ds.FileName)
	fr, err := filesystem.Open(ds.FileName)
	if err != nil {
		return fmt.Errorf("Failed to open file %s: %v", ds.FileName, err)
	}
	defer fr.Close()

	reader := NewReader(fr)
	if ds.HasHeader {
		reader.Read()
	}

	for {
		row, err := reader.Read()
		if err != nil {
			break
		}
		var oneRow []interface{}
		for _, field := range row {
			oneRow = append(oneRow, field)
		}
		gio.Emit(oneRow...)
	}

	return err
}

func decodeShardInfo(encodedShardInfo []byte) *CsvShardInfo {
	network := bytes.NewBuffer(encodedShardInfo)
	dec := gob.NewDecoder(network)
	var p CsvShardInfo
	if err := dec.Decode(&p); err != nil {
		log.Fatal("decode shard info", err)
	}
	return &p
}

func encodeShardInfo(shardInfo *CsvShardInfo) []byte {
	var network bytes.Buffer
	enc := gob.NewEncoder(&network)
	if err := enc.Encode(shardInfo); err != nil {
		log.Fatal("encode shard info:", err)
	}
	return network.Bytes()
}
