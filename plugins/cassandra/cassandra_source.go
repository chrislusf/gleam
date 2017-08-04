package cassandra

import (
	"fmt"
	"io"
	"math"
	"strings"
	"time"

	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/pb"
	"github.com/chrislusf/gleam/util"
	"github.com/gocql/gocql"
)

type CassandraSource struct {
	hosts            string
	Concurrency      int
	ShardCount       int
	LimitInEachShard int
	TimeoutSeconds   int

	prefix string

	selectClause string
	keyspace     string
	table        string
	whereClause  string
}

// Generate generates data shard info,
// partitions them via round robin,
// and reads each shard on each executor
func (s *CassandraSource) Generate(f *flow.Flow) *flow.Dataset {
	return s.genShardInfos(f).RoundRobin(s.prefix, s.Concurrency).Map(s.prefix+".Read", MapperReadShard)
}

func (s *CassandraSource) genShardInfos(f *flow.Flow) *flow.Dataset {
	return f.Source(s.prefix+".list", func(writer io.Writer, stats *pb.InstructionStat) error {

		hostList := strings.Split(s.hosts, ",")
		cluster := gocql.NewCluster(hostList...)
		cluster.Keyspace = s.keyspace
		cluster.ProtoVersion = 4
		cluster.Timeout = time.Duration(s.TimeoutSeconds) * time.Second

		// find out the partition keys
		session, err := cluster.CreateSession()
		if err != nil {
			return fmt.Errorf("genShardInfos create session %v keyspace %v: %v", s.hosts, s.keyspace, err)
		}
		defer session.Close()

		// println("driver Connected to", s.hosts, "keyspace", s.keyspace)

		keyspaceMetadata, err := session.KeyspaceMetadata(s.keyspace)
		if err != nil {
			return fmt.Errorf("Can not find keyspace %s", s.keyspace)
		}
		t, ok := keyspaceMetadata.Tables[s.table]
		if !ok {
			return fmt.Errorf("Can not find table %s in keyspace %s", s.table, s.keyspace)
		}
		var partitionKeys []string
		for _, column := range t.PartitionKey {
			partitionKeys = append(partitionKeys, column.Name)
		}

		// divide by token range
		if s.ShardCount == 0 {
			s.ShardCount = 32
		}
		var begin, end int64
		begin = math.MinInt64
		end = math.MaxInt64
		delta := end/int64(s.ShardCount) - begin/int64(s.ShardCount)
		for mype := int64(0); mype < int64(s.ShardCount); mype++ {
			start := begin + delta*mype
			stop := end
			if mype < int64(s.ShardCount)-1 {
				stop = begin + delta*(mype+1)
			}

			util.NewRow(util.Now(), encodeShardInfo(&CassandraShardInfo{
				Hosts:          s.hosts,
				Select:         s.selectClause,
				Keyspace:       s.keyspace,
				Table:          s.table,
				Where:          s.whereClause,
				Limit:          s.LimitInEachShard,
				TimeoutSeconds: s.TimeoutSeconds,
				PartitionKeys:  partitionKeys,
				StartToken:     fmt.Sprintf("%d", start),
				StopToken:      fmt.Sprintf("%d", stop),
			})).WriteTo(writer)
		}

		return nil
	})
}
