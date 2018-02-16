package mapreduce

import (
	"fmt"
	//"time"
	//
	"sync"
	"container/list"
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

// wangyu added:
type WorkerInfo struct {
	RPC_addr string
	has_done_work bool // whether the worker did some work or not.
	is_busy bool
	// the scheduler will give high priority to worker who has not done work and give them task first,
	// since this is a requirement of the test.
}

type WorkersPool struct {
	mu  sync.Mutex

	workersList [] WorkerInfo
	existsAvailableWorker *sync.Cond
	// signals when new workers becomes available
}


func newWorkersPool() (* WorkersPool) {
	nwp := new( WorkersPool)


	return nwp
}

func newWorkerHost(workersPool *WorkersPool, registerChan chan string, scheduleDone chan bool) {

	// this usage is insipred by forwardRegistrations in master.go

	for {
		// Use an external channel signal to stop a gorouitne:
		// https://stackoverflow.com/questions/6807590/how-to-stop-a-goroutine
		select {

		case newWorkerAddr := <-registerChan:

			newWorker := WorkerInfo{newWorkerAddr, false, false}

			fmt.Println("Worker %s added", newWorkerAddr)

			workersPool.mu.Lock()

				workersPool.workersList = append( workersPool.workersList, newWorker)
				workersPool.existsAvailableWorker = true

			workersPool.mu.Unlock()

		case <- scheduleDone:
			break
		}
	}

}


func assignWorkerTask(workerAddr string) {

}

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

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	// The master calls schedule() twice during a MapReduce job, once for the Map phase, and once for the Reduce phase.
	// schedule()'s job is to hand out tasks to the available workers.
	// There will usually be more tasks than worker threads,
	// so schedule() must give each worker a sequence of tasks, one at a time.

	// schedule() learns about the set of workers by reading its registerChan argument.


	// Fetch workers from channel

	// That channel yields a string for each worker, containing the worker's RPC address.
	// Some workers may exist before schedule() is called, and some may start while schedule() is running;
	// all will appear on registerChan.
	// schedule() should use all the workers, including ones that appear after it starts.

	// tasksDone := make([]bool, ntasks)

	tasksTodo := new(list.List)
	tasksDone := new(list.List)

	for i := 0; i < ntasks; i++ {
		tasksTodo.PushBack(i)
	}

	workersPool := newWorkersPool() // new() returns pointer type

	quit := make(chan bool) // the singal to stop


	go newWorkerHost( workersPool, registerChan, quit)

	// Assign task to workers as long as there is remaining tasks




	for tasksDone.Len() < ntasks {

		workersPool.mu.Lock()

		// If there is a (available) worker who has not done a task, choose that worker;
		selected := -1
		for i, _ := range workersPool.workersList {
			if workersPool.workersList[i].has_done_work {
				selected = i
				break
			}
		}
		// Otherwise choose an available worker.
		if selected == -1 {
			for i, _ := range workersPool.workersList {
				if !workersPool.workersList[i].is_busy {
					selected = i
					break
				}
			}
		}
		if selected != -1 {
			// found an available worker.
			workersPool.workersList[selected].is_busy = true
			go assignWorkerTask(workersPool.workersList[selected].RPC_addr)
		} else {
			// all workers are busy.
		}

		workersPool.mu.Unlock()

	}

	// schedule() tells a worker to execute a task by sending a Worker.DoTask RPC to the worker.
	// This RPC's arguments are defined by DoTaskArgs in mapreduce/common_rpc.go.
	// The File element is only used by Map tasks, and is the name of the file to read;
	// schedule() can find these file names in mapFiles.



	// Make RPC to give task to worker

	quit <- true // this makes sure newWorkerHost quits

	// schedule() should wait until all tasks have completed, and then return.


	//
	fmt.Printf("Schedule: %v done\n", phase)
}
