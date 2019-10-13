package mapreduce

import (
	"sync"

	"github.com/sirupsen/logrus"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	logrus.Debugf("Schedule %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	w := sync.WaitGroup{}
	w.Add(ntasks)
	for i := 0; i < ntasks; i++ {
		go func(i int) {
			args := DoTaskArgs{
				JobName:       jobName,
				File:          mapFiles[i],
				Phase:         phase,
				TaskNumber:    i,
				NumOtherPhase: n_other,
			}
			for {
				address := <-registerChan
				if result := call(address, "Worker.DoTask", args, nil); result {
					w.Done()
					registerChan <- address
					break
				}
			}
		}(i)
	}
	w.Wait()
	logrus.Debugf("Schedule %v done", phase)
}
