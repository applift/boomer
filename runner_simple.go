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

const StatsReportFile = "report.csv"

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

	statsReportFile := r.createStatsReport()
	if statsReportFile != nil {
		_, _ = statsReportFile.WriteString("time,name,rps,avg resp. time,max resp. time,min resp. time,num req,num failures\n")
	}
	go func() {
		for {
			select {
			case data := <-r.stats.messageToRunnerChan:
				r.logStats(data, statsReportFile)
			case <-r.stopChan:
				if statsReportFile != nil {
					statsReportFile.Close()
				}
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

func (r *SimpleRunner) createStatsReport() *os.File {
	if _, err := os.Stat(StatsReportFile); err == nil {
		err = os.Remove(StatsReportFile)
		if err != nil {
			fmt.Printf("Unable to remove file: %s\n", StatsReportFile)
		}
	}
	file, err := os.Create(StatsReportFile)
	if err != nil {
		fmt.Printf("Error while creating file: %v\n", err.Error())
	}
	return file
}

func (r *SimpleRunner) logStats(stats map[string]statsEntry, statsReport *os.File) {

	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered from crash while logging", r)
		}
	}()

	for _, sts := range stats {

		var reqPerSec = sts.numReqsPerSec

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
		if sts.numRequests == 0 {
			avgRespTime = 0
		} else {
			avgRespTime = sts.totalResponseTime / sts.numRequests
		}

		fmt.Printf("%s: request rate: %d, avg response time: %d\n", sts.name, avgReqPerSec, avgRespTime)
		if statsReport != nil {
			_, _ = statsReport.WriteString(fmt.Sprintf("%s,%s,%d,%d,%d,%d,%d,%d\n", time.Now().Format(time.RFC3339), sts.name, avgReqPerSec, avgRespTime, sts.maxResponseTime, sts.minResponseTime, sts.numRequests, sts.numFailures))
		}
	}
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

	remainingWeight := rand.Intn(weightSum) + 1

	var task *Task
	for _, t := range r.tasks {
		remainingWeight = remainingWeight - t.Weight
		if remainingWeight <= 0 {
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

	workers = int(r.requestRate)
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
