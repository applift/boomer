package boomer

import (
	"bytes"
	"testing"
	"unicode/utf8"
)

func TestPickWeightedRandomTask(t *testing.T) {
	var tasks = []*Task{
		&Task{
			Weight: 5,
			Fn:     func() {},
			Name:   "B",
		},
		&Task{
			Weight: 1,
			Fn:     func() {},
			Name:   "A",
		},
	}

	var runner = SimpleRunner{
		tasks:       tasks,
		rateLimiter: nil,
		stats:       nil,
		requestRate: 100,
		maxWorkers:  100,
		stopChan:    make(chan bool),
	}

	var trials = 10000
	var buffer bytes.Buffer
	for i := 0; i < trials; i++ {
		buffer.WriteString(runner.pickWeightedRandomTask().Name)
	}

	var str = buffer.String()

	var countA = count(str, "A")
	if countA > trials/5 || countA < trials/7 {
		t.Fatalf("From %d trials, %d returned task A", trials, countA)
	}
}

func count(str string, c string) int {
	count := 0
	for len(str) > 0 {
		r, size := utf8.DecodeLastRuneInString(str)
		if []rune(c)[0] == r {
			count++
		}
		str = str[:len(str)-size]
	}
	return count
}
