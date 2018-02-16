package mapreduce

import (
	"fmt"
	//"time"
	//
	"sync"
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


	// That channel yields a string for each worker, containing the worker's RPC address.
	// Some workers may exist before schedule() is called, and some may start while schedule() is running;
	// all will appear on registerChan.
	// schedule() should use all the workers, including ones that appear after it starts.

	// I am using registerChan as a pool of all workers.
	// Sub goroutines fetch worker from the pool when it needs.


	// https://golang.org/pkg/sync/#WaitGroup.Wait
	var wg sync.WaitGroup
	wg.Add(ntasks)

	for i := 0; i < ntasks; i++ {

		go func (i int, wg *sync.WaitGroup) {
			// make a copy of i to the func is really necessary!

			// this func is responsible for *completely* solve one task
			// thanks to a clever use of channel according to the hint.

			for {

				// fmt.Println("Job: %s:%d is waiting for worker", jobName, i)

				worker := <-registerChan

				// func (wk *Worker) DoTask(arg *DoTaskArgs, _ *struct{}) error {
				// wk.name, arg.Phase, arg.TaskNumber, arg.File, arg.NumOtherPhase)

				// JobName    string
				// File       string
				// Phase      jobPhase
				// TaskNumber int
				// NumOtherPhase int

				// fmt.Println("Worker %s is serving job: %s:%d", worker, jobName, i)

				arg := DoTaskArgs{jobName, mapFiles[i], phase, i, n_other}


				// Use the call() function in mapreduce/common_rpc.go to send an RPC to a worker.
				// The first argument is the the worker's address, as read from registerChan.
				// The second argument should be "Worker.DoTask".
				// The third argument should be the DoTaskArgs structure,
				// and the last argument should be nil.
				ok := call(worker, "Worker.DoTask", arg, nil)

				if ok {
					// a neat handling of worker failure.
					// failed worker will never be used again.
					go func (worker string) {
						// must do this in a goroutine, or it will deadlock since the works are not removed from this channel
						// until the schedule() is called the next time.
						// I *think* the worker's value has to be actually *copied* to make sure it exists after the
						// schedule() exists. though I did not test against it.
						registerChan <- worker
						}(worker)
					break
				}

			}

			//fmt.Println("Job %s finished", i)
			wg.Done()

		}(i, &wg) // it is important to copied the value of i.

	}

	wg.Wait()
	// schedule() should wait until all tasks have completed, and then return.

	//
	fmt.Printf("Schedule: %v done\n", phase)
}
