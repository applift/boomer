package boomer

import (
	"fmt"
	"os"
	"runtime/debug"
)

// Task is like the "Locust object" in locust, the python version.
// When boomer receives a start message from master, it will spawn several goroutines to run Task.Fn.
// But users can keep some information in the python version, they can't do the same things in boomer.
// Because Task.Fn is a pure function.
type Task struct {
	// The weight is used to distribute goroutines over multiple tasks.
	Weight int
	// Fn is called by the goroutines allocated to this task, in a loop.
	Fn   func()
	Name string
}

type Runner interface {
	run()

	stop()

	close()

	recordSuccess(requestType, name string, responseTime int64, responseLength int64)

	recordFailure(requestType, name string, responseTime int64, exception string)
}

// safeRun runs fn and recovers from unexpected panics.
// it prevents panics from Task.Fn crashing boomer.
func safeRun(fn func()) {
	defer func() {
		// don't panic
		err := recover()
		if err != nil {
			stackTrace := debug.Stack()
			errMsg := fmt.Sprintf("%v", err)
			os.Stderr.Write([]byte(errMsg))
			os.Stderr.Write([]byte("\n"))
			os.Stderr.Write(stackTrace)
			RecordFailure("unknown", "panic", int64(0), errMsg)
		}
	}()
	fn()
}
