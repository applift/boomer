package boomer

import (
	"time"
)

type requestSuccess struct {
	requestType    string
	name           string
	responseTime   int64
	responseLength int64
}

type requestFailure struct {
	requestType  string
	name         string
	responseTime int64
	error        string
}

type requestStats struct {
	entries   map[string]*statsEntry
	errors    map[string]*statsError
	total     *statsEntry
	startTime int64

	requestSuccessChan  chan *requestSuccess
	requestFailureChan  chan *requestFailure
	clearStatsChan      chan bool
	messageToRunnerChan chan map[string]*statsEntry
	shutdownChan        chan bool
}

func newRequestStats() (stats *requestStats) {
	entries := make(map[string]*statsEntry)
	errors := make(map[string]*statsError)

	stats = &requestStats{
		entries: entries,
		errors:  errors,
	}
	stats.requestSuccessChan = make(chan *requestSuccess, 100)
	stats.requestFailureChan = make(chan *requestFailure, 100)
	stats.clearStatsChan = make(chan bool)
	stats.messageToRunnerChan = make(chan map[string]*statsEntry, 10)
	stats.shutdownChan = make(chan bool)

	stats.total = &statsEntry{
		name:   "Total",
		method: "",
	}
	stats.total.reset()

	return stats
}

func (s *requestStats) logRequest(method, name string, responseTime int64, contentLength int64) {
	s.total.log(responseTime, contentLength)
	s.get(name, method).log(responseTime, contentLength)
}

func (s *requestStats) logError(method, name, err string) {
	s.total.logError(err)
	s.get(name, method).logError(err)

	// store error in errors map
	key := MD5(method, name, err)
	entry, ok := s.errors[key]
	if !ok {
		entry = &statsError{
			name:   name,
			method: method,
			error:  err,
		}
		s.errors[key] = entry
	}
	entry.occured()
}

func (s *requestStats) get(name string, method string) (entry *statsEntry) {
	entry, ok := s.entries[name+method]
	if !ok {
		newEntry := &statsEntry{
			name:          name,
			method:        method,
			numReqsPerSec: make(map[int64]int64),
			responseTimes: make(map[int64]int64),
		}
		newEntry.reset()
		s.entries[name+method] = newEntry
		return newEntry
	}
	return entry
}

func (s *requestStats) clearAll() {
	s.total = &statsEntry{
		name:   "Total",
		method: "",
	}
	s.total.reset()

	s.entries = make(map[string]*statsEntry)
	s.errors = make(map[string]*statsError)
	s.startTime = time.Now().Unix()
}

func (s *requestStats) start() {
	go func() {
		var ticker = time.NewTicker(slaveReportInterval)
		for {
			select {
			case m := <-s.requestSuccessChan:
				s.logRequest(m.requestType, m.name, m.responseTime, m.responseLength)
			case n := <-s.requestFailureChan:
				s.logError(n.requestType, n.name, n.error)
			case <-s.clearStatsChan:
				s.clearAll()
			case <-ticker.C:
				// send data to channel, no network IO in this goroutine
				s.messageToRunnerChan <- s.entries
				for _, v := range s.entries {
					v.reset()
				}
				s.total.reset()
				s.errors = make(map[string]*statsError)
			case <-s.shutdownChan:
				return
			}
		}
	}()
}

// close is used by unit tests to avoid leakage of goroutines
func (s *requestStats) close() {
	close(s.shutdownChan)
}

type statsEntry struct {
	name                 string
	method               string
	numRequests          int64
	numFailures          int64
	totalResponseTime    int64
	minResponseTime      int64
	maxResponseTime      int64
	numReqsPerSec        map[int64]int64
	responseTimes        map[int64]int64
	totalContentLength   int64
	startTime            int64
	lastRequestTimestamp int64
}

func (s *statsEntry) reset() {
	s.startTime = time.Now().Unix()
	s.numRequests = 0
	s.numFailures = 0
	s.totalResponseTime = 0
	s.responseTimes = make(map[int64]int64)
	s.minResponseTime = 0
	s.maxResponseTime = 0
	s.lastRequestTimestamp = time.Now().Unix()
	s.numReqsPerSec = make(map[int64]int64)
	s.totalContentLength = 0
}

func (s *statsEntry) log(responseTime int64, contentLength int64) {
	s.numRequests++

	s.logTimeOfRequest()
	s.logResponseTime(responseTime)

	s.totalContentLength += contentLength
}

func (s *statsEntry) logTimeOfRequest() {
	key := time.Now().Unix()
	_, ok := s.numReqsPerSec[key]
	if !ok {
		s.numReqsPerSec[key] = 1
	} else {
		s.numReqsPerSec[key]++
	}

	s.lastRequestTimestamp = key
}

func (s *statsEntry) logResponseTime(responseTime int64) {
	s.totalResponseTime += responseTime

	if s.minResponseTime == 0 {
		s.minResponseTime = responseTime
	}

	if responseTime < s.minResponseTime {
		s.minResponseTime = responseTime
	}

	if responseTime > s.maxResponseTime {
		s.maxResponseTime = responseTime
	}

	var roundedResponseTime int64

	// to avoid to much data that has to be transferred to the master node when
	// running in distributed mode, we save the response time rounded in a dict
	// so that 147 becomes 150, 3432 becomes 3400 and 58760 becomes 59000
	// see also locust's stats.py
	if responseTime < 100 {
		roundedResponseTime = responseTime
	} else if responseTime < 1000 {
		roundedResponseTime = int64(round(float64(responseTime), .5, -1))
	} else if responseTime < 10000 {
		roundedResponseTime = int64(round(float64(responseTime), .5, -2))
	} else {
		roundedResponseTime = int64(round(float64(responseTime), .5, -3))
	}

	_, ok := s.responseTimes[roundedResponseTime]
	if !ok {
		s.responseTimes[roundedResponseTime] = 1
	} else {
		s.responseTimes[roundedResponseTime]++
	}
}

func (s *statsEntry) logError(err string) {
	s.numFailures++
}

type statsError struct {
	name       string
	method     string
	error      string
	occurences int64
}

func (err *statsError) occured() {
	err.occurences++
}

func (err *statsError) toMap() map[string]interface{} {
	m := make(map[string]interface{})

	m["method"] = err.method
	m["name"] = err.name
	m["error"] = err.error
	m["occurences"] = err.occurences

	return m
}
