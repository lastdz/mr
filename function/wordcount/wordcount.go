package wordcount

import (
	"mapreduce"
	"strconv"
	"strings"
)

func Map(filename string, content string) []mapreduce.KeyValue {
	strs := strings.Split(content, " ")
	kvs := make([]mapreduce.KeyValue, 0)
	for _, str := range strs {
		kvs = append(kvs, mapreduce.KeyValue{
			Key:   str,
			Value: "1",
		})
	}
	return kvs
}

func Reduce(key string, values []string) string {
	return strconv.Itoa(len(values))
}
