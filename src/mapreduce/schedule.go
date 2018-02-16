package mapreduce

import (
	"fmt"
	//"time"
	//
	"sync"
	//"container/list"
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

	// Assign task to workers as long as there is remaining tasks

	var wg sync.WaitGroup
	// https://golang.org/pkg/sync/#WaitGroup.Wait
	wg.Add(ntasks)

	for i := 0; i < ntasks; i++ {

		go func (i int, wg *sync.WaitGroup) {

			// passing i to the func is really necessary!

			// this func is responsible for *completely* solve one task
			// thanks to a clever use of channel according to the hint.

			//for {

				fmt.Println("Job: %s:%d is waiting for worker", jobName, i)

				worker := <-registerChan

				// func (wk *Worker) DoTask(arg *DoTaskArgs, _ *struct{}) error {
				// wk.name, arg.Phase, arg.TaskNumber, arg.File, arg.NumOtherPhase)

				// JobName    string
				// File       string
				// Phase      jobPhase
				// TaskNumber int
				// NumOtherPhase int

				fmt.Println("Worker %s is serving job: %s:%d", worker, jobName, i)

				arg := DoTaskArgs{jobName, mapFiles[i], phase, i, n_other}

				// reply :=

				fmt.Println("RPC call")

				call(worker, "Worker.DoTask", &arg, new(struct{}))

				//if ok {
					registerChan <- worker
					//break;
				//}

			//}

			fmt.Println("Job %s finished", i)
			wg.Done()

		}(i, &wg)

	}

	// schedule() tells a worker to execute a task by sending a Worker.DoTask RPC to the worker.
	// This RPC's arguments are defined by DoTaskArgs in mapreduce/common_rpc.go.
	// The File element is only used by Map tasks, and is the name of the file to read;
	// schedule() can find these file names in mapFiles.

	wg.Wait()

	// Make RPC to give task to worker

	// schedule() should wait until all tasks have completed, and then return.


	//
	fmt.Printf("Schedule: %v done\n", phase)
}
