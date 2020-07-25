package mr

import (
	"strconv"
	"testing"
)

// func TestCompleteMap(t *testing.T) {
// 	intermediate := []KeyValue{{"a", "1"}, {"b", "1"}, {"a", "1"}}
// 	nReduce := 2
// 	num := 0
// 	completeMap(intermediate, nReduce, num)
// }

func TestCompleteRed(t *testing.T) {
	intermediate := []KeyValue{{"a", "1"}, {"b", "1"}, {"a", "1"}}
	nReduce := 1
	num := 0
	completeMap(intermediate, nReduce, num)

	reducef := func(key string, values []string) string { return strconv.Itoa(len(values)) }
	completeRed(num, reducef)
}
