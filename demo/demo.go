package main

import (
	"mapreduce"
	"mapreduce/function/wordcount"
	"time"
)

const (
	workerNums = 3
)

func main() {
	c := mapreduce.MakeCoordinator([]string{"testfile1.txt", "testfile2.txt", "testfile3.txt"}, 3)
	for i := 0; i < workerNums; i++ {
		go mapreduce.Worker(wordcount.Map, wordcount.Reduce)
	}
	for {
		if c.Done() {
			return
		}
		time.Sleep(time.Second)
	}
}
