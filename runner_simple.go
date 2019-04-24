package boomer

import (
	"fmt"
	"log"
	"math"
	"math/rand"
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

		r.spawnWorkers()
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

	var avgRespTime int64
	if statsTotal["num_requests"].(int64) == 0 {
		avgRespTime = 0
	} else {
		avgRespTime = statsTotal["total_response_time"].(int64) / statsTotal["num_requests"].(int64)
	}

	fmt.Println("Current request rate:", avgReqPerSec, ", avg response time:", avgRespTime, "ms")
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
	workerCount := r.getMaxWorkers()
	jobs := make(chan func(), workerCount*2)
	for w := 1; w <= workerCount; w++ {
		go r.worker(jobs)
	}

	//task loop inside another go routine so as not to block,
	//waiting is done inside Boomer
	go func() {
		for {
			select {
			case <-r.stopChan:
				// quit sending tasks to worker pool
				return
			default:
				task := r.pickWeightedRandomTask()
				//wait for rate limiter if necessary
				r.rateLimiter.Acquire()
				jobs <- task.Fn
			}
		}
	}()
}

func (r *SimpleRunner) worker(jobs <-chan func()) {
	for fn := range jobs {
		safeRun(fn)
	}
}

func (r *SimpleRunner) pickWeightedRandomTask() *Task {
	weightSum := 0
	for _, t := range r.tasks {
		weightSum += t.Weight
	}

	remainingWeight := rand.Intn(weightSum)

	var task *Task
	for _, t := range r.tasks {
		if remainingWeight = remainingWeight - t.Weight; remainingWeight <= 0 {
			task = t
			break
		}
	}
	return task
}

func (r *SimpleRunner) getMaxWorkers() (workers int) {
	var rLimit syscall.Rlimit
	err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		panic(fmt.Sprintf("Error Getting Rlimit: %s", err))
	}

	if r.maxWorkers > 0 {
		workers = int(math.Min(float64(r.maxWorkers), float64(r.requestRate)))
	}
	workers = int(math.Min(float64(workers), float64(rLimit.Cur-(rLimit.Cur/4))))
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
