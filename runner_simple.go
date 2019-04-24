package boomer

import (
	"fmt"
	"log"
	"math"
	"os"
	"syscall"
	"time"
)

type SimpleRunner struct {
	tasks       []*Task
	rateLimiter RateLimiter
	stats       *requestStats
	requestRate int64
	maxWorkers  int64
	stopChan    chan bool
}

func NewSimpleRunner(tasks []*Task) *Runner {
	var r Runner = &SimpleRunner{
		tasks:       tasks,
		rateLimiter: NewStableRateLimiter(maxRPS, time.Second),
		stats:       newRequestStats(),
		requestRate: maxRPS,
		maxWorkers:  maxWorkers,
		stopChan:    make(chan bool),
	}
	return &r
}

func (r *SimpleRunner) run() {
	r.stats.start()

	go func() {
		for {
			select {
			case data := <-r.stats.messageToRunnerChan:
				logStats(data)
			case <-r.stopChan:
				return
			}
		}
	}()

	if r.requestRate <= 0 {
		log.Printf("Invalid max_request rate %d\n", r.requestRate)
		os.Exit(-1)
	} else {
		Events.Publish("boomer:hatch", r.requestRate, r.requestRate)

		r.rateLimiter.Start()
		r.stats.clearStatsChan <- true

		r.stopChan = make(chan bool)

		go r.spawnWorkers()
	}
}

func logStats(stats map[string]interface{}) {

	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered from crash while logging", r)
		}
	}()

	var statsTotal = stats["stats_total"].(map[string]interface{})
	var reqPerSec = statsTotal["num_reqs_per_sec"].(map[int64]int64)

	var total int64
	var count int64
	for _, v := range reqPerSec {
		count++
		total += v
	}

	var avgReqPerSec int64
	if count == 0 {
		avgReqPerSec = 0
	} else {
		avgReqPerSec = total / count
	}

	var respTimes = statsTotal["response_times"].(map[int64]int64)

	total = 0
	count = 0
	for k, v := range respTimes {
		count += v
		total += (v * k)
	}
	var avgRespTime int64
	if count == 0 {
		avgRespTime = 0
	} else {
		avgRespTime = total / count
	}

	var fromTotalAvgRespTime int64
	if statsTotal["num_requests"].(int64) == 0 {
		fromTotalAvgRespTime = 0
	} else {
		fromTotalAvgRespTime = statsTotal["total_response_time"].(int64) / statsTotal["num_requests"].(int64)
	}

	fmt.Println("Current request rate:", avgReqPerSec, ", avg response time:", avgRespTime, "ms, (", fromTotalAvgRespTime, "ms)")
}

func (r *SimpleRunner) recordSuccess(requestType, name string, responseTime int64, responseLength int64) {
	r.stats.requestSuccessChan <- &requestSuccess{
		requestType:    requestType,
		name:           name,
		responseTime:   responseTime,
		responseLength: responseLength,
	}
}

func (r *SimpleRunner) recordFailure(requestType, name string, responseTime int64, exception string) {
	r.stats.requestFailureChan <- &requestFailure{
		requestType:  requestType,
		name:         name,
		responseTime: responseTime,
		error:        exception,
	}
}

func (r *SimpleRunner) spawnWorkers() {
	workers := r.getMaxWorkers()

	weightSum := 0
	for _, task := range r.tasks {
		weightSum += task.Weight
	}

	for _, task := range r.tasks {
		percent := float64(task.Weight) / float64(weightSum)
		workersForTask := int64(round(workers*percent, .5, 0))

		if weightSum == 0 {
			workersForTask = int64(workers / float64(len(r.tasks)))
		}

		for i := int64(1); i <= workersForTask; i++ {
			if i%r.requestRate == 0 {
				time.Sleep(1 * time.Second)
			}

			select {
			case <-r.stopChan:
				// quit hatching goroutine
				return
			default:
				go func(fn func()) {
					for {
						select {
						case <-r.stopChan:
							return
						default:
							blocked := r.rateLimiter.Acquire()
							if !blocked {
								safeRun(fn)
							}
						}
					}
				}(task.Fn)
			}
		}
	}
}

func (r *SimpleRunner) getMaxWorkers() (workers float64) {
	var rLimit syscall.Rlimit
	err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		panic(fmt.Sprintf("Error Getting Rlimit: %s", err))
	}

	if r.maxWorkers > 0 {
		workers = math.Min(float64(r.maxWorkers), float64(r.requestRate))
	}
	workers = math.Min(workers, float64(rLimit.Cur-(rLimit.Cur/4)))
	log.Println("Spawning", workers, "workers to maintain", r.requestRate, "requests/s...")
	return
}

func (r *SimpleRunner) stop() {
	fmt.Println("Stopping runner")
	r.stopChan <- true
	close(r.stopChan)
}

func (r *SimpleRunner) close() {
	fmt.Println("Closing runner")
	r.stop()
}
