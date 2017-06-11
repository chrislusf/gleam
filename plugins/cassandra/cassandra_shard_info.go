package cassandra

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"reflect"
	"strings"

	"github.com/chrislusf/gleam/gio"
	"github.com/gocql/gocql"
)

type CassandraShardInfo struct {
	Hosts                 string
	StartToken, StopToken string
	PartitionKeys         []string

	Select   string
	Keyspace string
	Table    string
	Where    string
}

var (
	MapperReadShard = gio.RegisterMapper(readShard)
)

func init() {
	gob.Register(CassandraShardInfo{})
}

func readShard(row []interface{}) error {
	encodedShardInfo := row[0].([]byte)
	return decodeShardInfo(encodedShardInfo).ReadSplit()
}

func (s *CassandraShardInfo) ReadSplit() error {

	cluster := gocql.NewCluster(strings.Split(s.Hosts, ",")...)
	cluster.Keyspace = s.Keyspace
	cluster.ProtoVersion = 4

	session, err := cluster.CreateSession()
	if err != nil {
		return fmt.Errorf("Failed to create cassandra session when ReadSplit: %v", err)
	}
	defer session.Close()

	partitionKeys := strings.Join(s.PartitionKeys, ",")
	table := s.Table
	if s.Keyspace != "" {
		table = s.Keyspace + "." + table
	}
	cql := fmt.Sprintf("select %s from %s where Token(%s) > %s AND Token(%s) <= %s ",
		s.Select, table, partitionKeys, s.StartToken, partitionKeys, s.StopToken)

	if s.Where != "" {
		cql = cql + " AND " + s.Where
	}

	// println("cql:", cql)

	iter := session.Query(cql).Iter()

	var values []interface{}
	columns := iter.Columns()
	for _, c := range columns {
		values = append(values, c.TypeInfo.New())
	}
	objects := make([]interface{}, len(values))

	for iter.Scan(values...) {
		for i, v := range values {
			objects[i] = reflect.Indirect(reflect.ValueOf(v)).Interface()
		}
		gio.Emit(objects...)
	}

	if err := iter.Close(); err != nil {
		return fmt.Errorf("Failed to iterate the data: %v", err)
	}

	return nil

}

func decodeShardInfo(encodedShardInfo []byte) *CassandraShardInfo {
	network := bytes.NewBuffer(encodedShardInfo)
	dec := gob.NewDecoder(network)
	var p CassandraShardInfo
	if err := dec.Decode(&p); err != nil {
		log.Fatal("decode shard info", err)
	}
	return &p
}

func encodeShardInfo(shardInfo *CassandraShardInfo) []byte {
	var network bytes.Buffer
	enc := gob.NewEncoder(&network)
	if err := enc.Encode(shardInfo); err != nil {
		log.Fatal("encode shard info:", err)
	}
	return network.Bytes()
}
