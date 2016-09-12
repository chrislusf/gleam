// priority_queue_test.go
package util

import (
	"math/rand"
	"reflect"
	"testing"
)

func TestSortedData(t *testing.T) {
	data := make([]int32, 102400)
	for i := 0; i < 100; i++ {
		data[i] = rand.Int31n(15)
	}
	pq := NewPriorityQueue(func(a, b interface{}) bool {
		aVal, bVal := a.(reflect.Value), b.(reflect.Value)
		return aVal.Int() < bVal.Int()
	})
	for i := 0; i < 10; i++ {
		pq.Enqueue(reflect.ValueOf(rand.Int31n(5606)), i)
	}
	for pq.Len() > 0 {
		t, i := pq.Dequeue()
		println(t.(reflect.Value).Int(), i)
	}
}
