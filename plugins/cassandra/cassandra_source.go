package cassandra

import (
	"fmt"
	"io"
	"math"
	"strings"

	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/util"
	"github.com/gocql/gocql"
)

type CassandraSource struct {
	Hosts          string
	PartitionCount int

	Select   string
	Keyspace string
	Table    string
	Where    string

	cluster *gocql.ClusterConfig
}

// Generate generates data shard info,
// partitions them via round robin,
// and reads each shard on each executor
func (s *CassandraSource) Generate(f *flow.Flow) *flow.Dataset {
	return s.genShardInfos(f).RoundRobin(s.PartitionCount).Mapper(MapperReadShard)
}

// New creates a CassandraSource.
// format: select {selectClause} from {keyspace}.{table} where {whereClause}"
func New(hosts string, keyspace string, selectClause, table, whereClause string, partitionCount int) *CassandraSource {
	s := &CassandraSource{
		Hosts:          hosts,
		Select:         selectClause,
		Keyspace:       keyspace,
		Table:          table,
		Where:          whereClause,
		PartitionCount: partitionCount,
	}
	hostList := strings.Split(hosts, ",")
	s.cluster = gocql.NewCluster(hostList...)
	s.cluster.Keyspace = keyspace
	s.cluster.ProtoVersion = 4

	return s
}

func (s *CassandraSource) genShardInfos(f *flow.Flow) *flow.Dataset {
	return f.Source(func(writer io.Writer) error {

		// find out the partition keys
		session, err := s.cluster.CreateSession()
		if err != nil {
			return fmt.Errorf("Failed to create cassandra session when GetSplits: %v", err)
		}
		defer session.Close()
		keyspaceMetadata, err := session.KeyspaceMetadata(s.Keyspace)
		if err != nil {
			return fmt.Errorf("Can not find keyspace %s", s.Keyspace)
		}
		t, ok := keyspaceMetadata.Tables[s.Table]
		if !ok {
			return fmt.Errorf("Can not find table %s in keyspace %s", s.Table, s.Keyspace)
		}
		var partitionKeys []string
		for _, column := range t.PartitionKey {
			partitionKeys = append(partitionKeys, column.Name)
		}

		// divide by token range
		if s.PartitionCount == 0 {
			s.PartitionCount = 32
		}
		var begin, end int64
		begin = math.MinInt64
		end = math.MaxInt64
		delta := end/int64(s.PartitionCount) - begin/int64(s.PartitionCount)
		for mype := int64(0); mype < int64(s.PartitionCount); mype++ {
			start := begin + delta*mype
			stop := end
			if mype < int64(s.PartitionCount)-1 {
				stop = begin + delta*(mype+1)
			}
			util.WriteRow(writer, encodeShardInfo(&CassandraShardInfo{
				Hosts:         s.Hosts,
				Select:        s.Select,
				Keyspace:      s.Keyspace,
				Table:         s.Table,
				Where:         s.Where,
				PartitionKeys: partitionKeys,
				StartToken:    fmt.Sprintf("%d", start),
				StopToken:     fmt.Sprintf("%d", stop),
			}))
		}

		return nil
	})
}
