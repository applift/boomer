package boomer

import (
	"log"
	"sync/atomic"
	"time"
)

const (
	stateInit     = "ready"
	stateHatching = "hatching"
	stateRunning  = "running"
	stateStopped  = "stopped"
	stateQuitting = "quitting"
)

const (
	slaveReportInterval = 3 * time.Second
	heartbeatInterval   = 1 * time.Second
)

// MasterRunner is the most important component of boomer.
// It connects to the master, spawns goroutines and collects stats.
type MasterRunner struct {
	nodeID     string
	masterHost string
	masterPort int
	hatchType  string
	state      string
	tasks      []*Task

	client           client
	rateLimiter      RateLimiter
	rateLimitEnabled bool
	stats            *requestStats

	numClients int32
	hatchRate  int

	// all running workers(goroutines) will select on this channel.
	// close this channel will stop all running workers.
	stopChan chan bool

	// close this channel will stop all goroutines used in masterRunner.
	closeChan chan bool
}

func NewMasterRunner(tasks ...*Task) Runner {
	var r Runner = &MasterRunner{
		masterHost: masterHost,
		masterPort: masterPort,
		nodeID:     getNodeID(),
		tasks:      tasks,
		hatchType:  hatchType,
		closeChan:  make(chan bool),
		stats:      newRequestStats(),
	}

	rateLimiter, err := createRateLimiter(maxRPS, requestIncreaseRate)
	if err != nil {
		log.Fatalf("%v\n", err)
	}

	if rateLimiter != nil {
		r.(*MasterRunner).rateLimitEnabled = true
		r.(*MasterRunner).rateLimiter = rateLimiter
	}

	return r
}

func (r *MasterRunner) run() {
	r.state = stateInit
	r.client = newClient(r.masterHost, r.masterPort, r.nodeID)
	r.client.connect()

	// listen to master
	r.startListener()

	r.stats.start()

	// tell master, I'm ready
	r.client.sendChannel() <- newMessage("client_ready", nil, r.nodeID)

	// report to master
	// TODO reintroduce serialisation logic deleted from stats.go
	// go func() {
	// 	for {
	// 		select {
	// 		case data := <-r.stats.messageToRunnerChan:
	// 			if r.state == stateInit || r.state == stateStopped {
	// 				continue
	// 			}
	// 			data["user_count"] = r.numClients
	// 			r.client.sendChannel() <- newMessage("stats", data, r.nodeID)
	// 		case <-r.closeChan:
	// 			return
	// 		}
	// 	}
	// }()

	// heartbeat
	// See: https://github.com/locustio/locust/commit/a8c0d7d8c588f3980303358298870f2ea394ab93
	go func() {
		var ticker = time.NewTicker(heartbeatInterval)
		for {
			select {
			case <-ticker.C:
				data := map[string]interface{}{
					"state": r.state,
				}
				r.client.sendChannel() <- newMessage("heartbeat", data, r.nodeID)
			case <-r.closeChan:
				return
			}
		}
	}()

	Events.Subscribe("boomer:quit", r.onQuiting)
}

func (r *MasterRunner) recordSuccess(requestType, name string, responseTime int64, responseLength int64) {
	r.stats.requestSuccessChan <- &requestSuccess{
		requestType:    requestType,
		name:           name,
		responseTime:   responseTime,
		responseLength: responseLength,
	}
}

func (r *MasterRunner) recordFailure(requestType, name string, responseTime int64, exception string) {
	r.stats.requestFailureChan <- &requestFailure{
		requestType:  requestType,
		name:         name,
		responseTime: responseTime,
		error:        exception,
	}
}

func (r *MasterRunner) startListener() {
	go func() {
		for {
			select {
			case msg := <-r.client.recvChannel():
				r.onMessage(msg)
			case <-r.closeChan:
				return
			}
		}
	}()
}

// MasterRunner acts as a state machine.
func (r *MasterRunner) onMessage(msg *message) {
	switch r.state {
	case stateInit:
		switch msg.Type {
		case "hatch":
			r.state = stateHatching
			r.onHatchMessage(msg)
		case "quit":
			Events.Publish("boomer:quit")
		}
	case stateHatching:
		fallthrough
	case stateRunning:
		switch msg.Type {
		case "hatch":
			r.state = stateHatching
			r.stop()
			r.onHatchMessage(msg)
		case "stop":
			r.stop()
			r.state = stateStopped
			log.Println("Recv stop message from master, all the goroutines are stopped")
			r.client.sendChannel() <- newMessage("client_stopped", nil, r.nodeID)
			r.client.sendChannel() <- newMessage("client_ready", nil, r.nodeID)
			r.state = stateInit
		case "quit":
			r.stop()
			log.Println("Recv quit message from master, all the goroutines are stopped")
			Events.Publish("boomer:quit")
			r.state = stateInit
		}
	case stateStopped:
		switch msg.Type {
		case "hatch":
			r.state = stateHatching
			r.onHatchMessage(msg)
		case "quit":
			Events.Publish("boomer:quit")
			r.state = stateInit
		}
	}
}

func (r *MasterRunner) onHatchMessage(msg *message) {
	r.client.sendChannel() <- newMessage("hatching", nil, r.nodeID)
	rate, _ := msg.Data["hatch_rate"]
	clients, _ := msg.Data["num_clients"]
	hatchRate := int(rate.(float64))
	workers := 0
	if _, ok := clients.(uint64); ok {
		workers = int(clients.(uint64))
	} else {
		workers = int(clients.(int64))
	}
	if workers == 0 || hatchRate == 0 {
		log.Printf("Invalid hatch message from master, num_clients is %d, hatch_rate is %d\n",
			workers, hatchRate)
	} else {
		Events.Publish("boomer:hatch", workers, hatchRate)

		if r.rateLimitEnabled {
			r.rateLimiter.Start()
		}
		r.startHatching(workers, hatchRate)
	}
}

func (r *MasterRunner) startHatching(spawnCount int, hatchRate int) {
	r.stats.clearStatsChan <- true
	r.stopChan = make(chan bool)

	r.hatchRate = hatchRate
	r.numClients = 0
	go r.spawnWorkers(spawnCount, r.stopChan)
}

func (r *MasterRunner) spawnWorkers(spawnCount int, quit chan bool) {
	log.Println("Hatching and swarming", spawnCount, "clients at the rate", r.hatchRate, "clients/s...")

	weightSum := 0
	for _, task := range r.tasks {
		weightSum += task.Weight
	}

	for _, task := range r.tasks {
		percent := float64(task.Weight) / float64(weightSum)
		amount := int(round(float64(spawnCount)*percent, .5, 0))

		if weightSum == 0 {
			amount = int(float64(spawnCount) / float64(len(r.tasks)))
		}

		for i := 1; i <= amount; i++ {
			if r.hatchType == "smooth" {
				time.Sleep(time.Duration(1000000/r.hatchRate) * time.Microsecond)
			} else if i%r.hatchRate == 0 {
				time.Sleep(1 * time.Second)
			}

			select {
			case <-quit:
				// quit hatching goroutine
				return
			default:
				atomic.AddInt32(&r.numClients, 1)
				go func(fn func()) {
					for {
						select {
						case <-quit:
							return
						default:
							if r.rateLimitEnabled {
								blocked := r.rateLimiter.Acquire()
								if !blocked {
									safeRun(fn)
								}
							} else {
								safeRun(fn)
							}
						}
					}
				}(task.Fn)
			}
		}
	}

	r.hatchComplete()
}

func (r *MasterRunner) hatchComplete() {
	data := make(map[string]interface{})
	data["count"] = r.numClients
	r.client.sendChannel() <- newMessage("hatch_complete", data, r.nodeID)
	r.state = stateRunning
}

func (r *MasterRunner) onQuiting() {
	if r.state != stateQuitting {
		r.client.sendChannel() <- newMessage("quit", nil, r.nodeID)
	}
}

func (r *MasterRunner) stop() {
	// publish the boomer stop event
	// user's code can subscribe to this event and do thins like cleaning up
	Events.Publish("boomer:stop")

	// stop previous goroutines without blocking
	// those goroutines will exit when r.safeRun returns
	close(r.stopChan)
	if r.rateLimitEnabled {
		r.rateLimiter.Stop()
	}
}

func (r *MasterRunner) close() {
	var ticker = time.NewTicker(3 * time.Second)

	// wait for quit message is sent to master
	select {
	case <-r.client.disconnectedChannel():
		break
	case <-ticker.C:
		log.Println("Timeout waiting for sending quit message to master, boomer will quit any way.")
		break
	}

	if r.stats != nil {
		r.stats.close()
	}
	if r.client != nil {
		r.client.close()
	}
	close(r.closeChan)
}
