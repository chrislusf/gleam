package cassandra

import (
	"encoding/gob"
	"fmt"
	"io"
	"reflect"
	"strings"

	"github.com/chrislusf/gleam/adapter"
	"github.com/chrislusf/gleam/util"
	"github.com/gocql/gocql"
)

func init() {
	gob.Register(CassandraDataSplit{})

	adapter.RegisterAdapter(NewCassandraAdapter())
}

type Query struct {
	Select    string
	Keyspace  string
	Table     string
	Where     string
	Partition int
	Parallel  int
}

type CassandraDataSplit struct {
	Config                         map[string]string
	StartToken, StopToken          string
	Select, Keyspace, Table, Where string
	PartitionKeys                  []string
}

func (q Query) GetParallelCount() int {
	return q.Parallel
}

type CassandraAdapter struct {
	cluster *gocql.ClusterConfig
}

func NewCassandraAdapter() *CassandraAdapter {
	return &CassandraAdapter{}
}

func (c *CassandraAdapter) AdapterName() string {
	return "cassandra"
}

func (c *CassandraAdapter) LoadConfiguration(config map[string]string) {
	hosts := strings.Split(config["hosts"], ",")
	c.cluster = gocql.NewCluster(hosts...)
	c.cluster.Keyspace = config["keyspace"]
	c.cluster.ProtoVersion = 4
}

func (c *CassandraAdapter) ReadSplit(split adapter.Split, writer io.Writer) error {
	session, err := c.cluster.CreateSession()
	if err != nil {
		return fmt.Errorf("Failed to create cassandra session when ReadSplit: %v", err)
	}
	defer session.Close()

	ds, isCassandraDataSplit := split.(CassandraDataSplit)
	if !isCassandraDataSplit {
		return fmt.Errorf("split is not CassandraDataSplit? %v", split)
	}

	partitionKeys := strings.Join(ds.PartitionKeys, ",")
	table := ds.Table
	if ds.Keyspace != "" {
		table = ds.Keyspace + "." + table
	}
	cql := fmt.Sprintf("select %s from %s where Token(%s) > %s AND Token(%s) <= %s ",
		ds.Select, table, partitionKeys, ds.StartToken, partitionKeys, ds.StopToken)

	if ds.Where != "" {
		cql = cql + " AND " + ds.Where
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
		util.WriteRow(writer, objects...)
	}

	if err := iter.Close(); err != nil {
		return fmt.Errorf("Failed to iterate the data: %v", err)
	}
	return nil
}

func (cs CassandraDataSplit) GetConfiguration() map[string]string {
	return cs.Config
}
