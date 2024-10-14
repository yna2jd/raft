package mapreduce

import (
	"sync"
)

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}
	wkWaitGroup := sync.WaitGroup{}
	debug("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)
	for i := 0; i < ntasks; i++ {
		addr := <-mr.registerChannel
		task := i
		go func(address string, task int) {
			wkWaitGroup.Add(1)
			for {
				args := DoTaskArgs{
					mr.jobName,
					mr.files[task],
					phase,
					task,
					nios,
				}
				success := call(address, "Worker.DoTask", &args, nil)
				if success {
					go func(addr string) {
						mr.registerChannel <- addr
						wkWaitGroup.Done()
					}(address)
					return
				} else {
					address = <-mr.registerChannel
				}
			}
		}(addr, task)
	}
	debug("Schedule: %v phase done\n", phase)
}
