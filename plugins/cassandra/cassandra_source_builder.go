package cassandra

/*
This file is only for the builder API.
*/

func Hosts(hosts string) *CassandraSource {
	return &CassandraSource{
		hosts:        hosts,
		keyspace:     "system",
		table:        "local",
		selectClause: "*",
		ShardCount:   32,
		Concurrency:  4,
		prefix:       "cassandra",
	}
}

func (s *CassandraSource) Keyspace(keyspace string) *CassandraSource {
	s.keyspace = keyspace
	return s
}

func (s *CassandraSource) From(table string) *CassandraSource {
	s.table = table
	return s
}

func (s *CassandraSource) Select(selectClause string) *CassandraSource {
	s.selectClause = selectClause
	return s
}

func (s *CassandraSource) Where(whereClause string) *CassandraSource {
	s.whereClause = whereClause
	return s
}
