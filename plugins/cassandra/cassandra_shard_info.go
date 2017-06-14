package cassandra

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"

	"github.com/chrislusf/gleam/gio"
	"github.com/gocql/gocql"
)

type CassandraShardInfo struct {
	Hosts                 string
	StartToken, StopToken string
	PartitionKeys         []string
	TimeoutSeconds        int

	Select   string
	Keyspace string
	Table    string
	Where    string
	Limit    int
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

	// println("hosts:", s.Hosts)

	cluster := gocql.NewCluster(strings.Split(s.Hosts, ",")...)
	cluster.Keyspace = s.Keyspace
	cluster.ProtoVersion = 4
	cluster.Timeout = time.Duration(s.TimeoutSeconds) * time.Second
	cluster.ConnectTimeout = time.Duration(s.TimeoutSeconds) * time.Second

	session, err := cluster.CreateSession()
	if err != nil {
		return fmt.Errorf("ReadSplit connect to %s %s: %v", s.Hosts, s.Keyspace, err)
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

	if s.Limit != 0 {
		cql = fmt.Sprintf("%s LIMIT %d", cql, s.Limit)
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
