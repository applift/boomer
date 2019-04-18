package boomer

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/asaskevich/EventBus"
)

// Events is the global event bus instance.
var Events = EventBus.New()

var defaultBoomer *Boomer

// A Boomer is used to run tasks.
// This type is exposed, so users can create and control a Boomer instance programmatically.
type Boomer struct {
	hatchType   string
	rateLimiter RateLimiter
	runner      Runner
}

// SetHatchType only accepts "asap" or "smooth".
// "asap" means spawning goroutines as soon as possible when the test is started.
// "smooth" means a constant pace.
func (b *Boomer) SetHatchType(hatchType string) {
	if hatchType != "asap" && hatchType != "smooth" {
		log.Printf("Wrong hatch-type, expected asap or smooth, was %s\n", hatchType)
		return
	}
	b.hatchType = hatchType
}

// Run accepts a slice of Task and connects to the locust master.
func (b *Boomer) Run(tasks ...*Task) {
	b.runner.run()
}

// RecordSuccess reports a success.
func (b *Boomer) RecordSuccess(requestType, name string, responseTime int64, responseLength int64) {
	if b.runner == nil {
		return
	}
	b.runner.recordSuccess(requestType, name, responseTime, responseLength)
}

// RecordFailure reports a failure.
func (b *Boomer) RecordFailure(requestType, name string, responseTime int64, exception string) {
	if b.runner == nil {
		return
	}
	b.runner.recordFailure(requestType, name, responseTime, exception)
}

// Quit will send a quit message to the master.
func (b *Boomer) Quit() {
	Events.Publish("boomer:quit")
	b.runner.close()
}

// Run tasks without connecting to the master.
func runTasksForTest(tasks ...*Task) {
	taskNames := strings.Split(runTasks, ",")
	for _, task := range tasks {
		if task.Name == "" {
			continue
		} else {
			for _, name := range taskNames {
				if name == task.Name {
					log.Println("Running " + task.Name)
					task.Fn()
				}
			}
		}
	}
}

// Run accepts a slice of Task and connects to a locust master.
// It's a convenience function to use the defaultBoomer.
func Run(r Runner, tasks ...*Task) {
	if r == nil {
		r = NewMasterRunner(tasks...)
	}

	defaultBoomer = &Boomer{
		runner:    r,
		hatchType: "asap",
	}

	if !flag.Parsed() {
		flag.Parse()
	}

	if runTasks != "" {
		runTasksForTest(tasks...)
		return
	}

	initLegacyEventHandlers()

	if memoryProfile != "" {
		StartMemoryProfile(memoryProfile, memoryProfileDuration)
	}

	if cpuProfile != "" {
		StartCPUProfile(cpuProfile, cpuProfileDuration)
	}

	defaultBoomer.hatchType = hatchType

	defaultBoomer.Run(tasks...)

	quitByMe := false
	Events.Subscribe("boomer:quit", func() {
		if !quitByMe {
			log.Println("shut down")
			os.Exit(0)
		}
	})

	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)

	<-c
	quitByMe = true
	defaultBoomer.Quit()

	log.Println("shut down")
}

// RecordSuccess reports a success.
// It's a convenience function to use the defaultBoomer.
func RecordSuccess(requestType, name string, responseTime int64, responseLength int64) {
	defaultBoomer.RecordSuccess(requestType, name, responseTime, responseLength)
}

// RecordFailure reports a failure.
// It's a convenience function to use the defaultBoomer.
func RecordFailure(requestType, name string, responseTime int64, exception string) {
	defaultBoomer.RecordFailure(requestType, name, responseTime, exception)
}
