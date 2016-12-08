package cassandra

import (
	"fmt"
	"math"

	"github.com/chrislusf/gleam/adapter"
)

func (c *CassandraAdapter) GetSplits(connectionId string, aq adapter.AdapterQuery) (splits []adapter.Split, err error) {

	query, isCassandraQuery := aq.(*Query)
	if !isCassandraQuery {
		return nil, fmt.Errorf("input for GetSplits() is not cassandra query? %v", aq)
	}

	connectionInfo, ok := adapter.ConnectionManager.GetConnectionInfo(connectionId)
	if !ok {
		return nil, fmt.Errorf("Failed to find configuration for %s.", connectionId)
	}
	c.LoadConfiguration(connectionInfo.GetConfig())

	// find out the partition keys
	session, err := c.cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("Failed to create cassandra session when GetSplits: %v", err)
	}
	defer session.Close()
	keyspaceMetadata, err := session.KeyspaceMetadata(query.Keyspace)
	if err != nil {
		return nil, fmt.Errorf("Can not find keyspace %s", query.Keyspace)
	}
	t, ok := keyspaceMetadata.Tables[query.Table]
	if !ok {
		return nil, fmt.Errorf("Can not find table %s in keyspace %s", query.Table, query.Keyspace)
	}
	var partitionKeys []string
	for _, column := range t.PartitionKey {
		partitionKeys = append(partitionKeys, column.Name)
	}

	// divide by token range
	if query.Partition == 0 {
		query.Partition = 32
	}
	var begin, end int64
	begin = math.MinInt64
	end = math.MaxInt64
	delta := end/int64(query.Partition) - begin/int64(query.Partition)
	for mype := int64(0); mype < int64(query.Partition); mype++ {
		start := begin + delta*mype
		stop := end
		if mype < int64(query.Partition)-1 {
			stop = begin + delta*(mype+1)
		}
		splits = append(splits, &CassandraDataSplit{
			Config:        connectionInfo.GetConfig(),
			Select:        query.Select,
			Keyspace:      query.Keyspace,
			Table:         query.Table,
			Where:         query.Where,
			PartitionKeys: partitionKeys,
			StartToken:    fmt.Sprintf("%d", start),
			StopToken:     fmt.Sprintf("%d", stop),
		})
	}

	return
}
