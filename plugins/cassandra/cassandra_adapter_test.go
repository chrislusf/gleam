package cassandra

import (
	"fmt"
	"log"
	"reflect"
	"testing"

	"github.com/gocql/gocql"
)

func TestExport(t *testing.T) {
	cluster := gocql.NewCluster("localhost")
	cluster.Keyspace = "system"
	cluster.ProtoVersion = 4
	session, err := cluster.CreateSession()
	if err != nil {
		log.Printf("Failed to create cassandra session: %v", err)
		return
	}
	defer session.Close()

	iter := session.Query("SELECT * FROM local").Iter()

	columns := iter.Columns()
	for _, c := range columns {
		fmt.Printf("%v,", c.Name)
	}
	fmt.Println()

	var values []interface{}
	for _, c := range columns {
		values = append(values, c.TypeInfo.New())
	}

	for iter.Scan(values...) {
		for _, v := range values {
			fmt.Printf("%v,", reflect.Indirect(reflect.ValueOf(v)))
		}
		fmt.Println()
	}

	if err := iter.Close(); err != nil {
		log.Fatal(err)
	}

}
