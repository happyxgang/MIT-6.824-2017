package mapreduce

import (
  "fmt"
  "sync"

)
type JobInfo struct {
  file string
  index int
  worker string
  status int
}
type JobManager struct{
  jobInfo map[string]*JobInfo
}
func (jobMng *JobManager)Init(files []string){
  jobMng.jobInfo = make(map[string]*JobInfo)
  index :=0
  for _,f := range files {
    jobMng.jobInfo[f] = &JobInfo{f, index, "", 0}
    index = index+1
  }
}
func (jobMng *JobManager)GetJob()(job *JobInfo,allDone bool){
  allDone = true
  for _,job:= range jobMng.jobInfo {
    if job.status ==0 || job.status == 1 {
      allDone = false
    }
    if job.status == 0 {
      return job, allDone
    }
  }
  return nil, allDone
}
func (jobMng *JobManager)SetStart(file string, worker string){
  jobMng.jobInfo[file].worker =  worker
  jobMng.jobInfo[file].status = 1
}
func (jobMng *JobManager)SetFinished(file string, worker string){
  //if jobMng.jobInfo[file].worker == worker {
  jobMng.jobInfo[file].status = 2
  //}
}
func (jobMng *JobManager)ResetJob(file string, worker string){
	fmt.Printf("Reset record worker:%s, now :%s\n",jobMng.jobInfo[file].worker, worker)
  if jobMng.jobInfo[file].worker == worker {
    jobMng.jobInfo[file].status = 0
  }
}
func (jobMng *JobManager)GetJobNumber()int{
	return len(jobMng.jobInfo)
}

func (jobMng *JobManager)GetFinishJobNumber()int{
	done := 0
	for _, job := range jobMng.jobInfo{
		if job.status == 2{
			done += 1
		}
	}
	return done
}


type TaskReq struct{
  end bool
  file string
  index int
}
type TaskRsp struct {
  status int
  file string
  worker string
}


//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
type WorkerInfo struct {
	workchan chan TaskReq
	busy bool
}
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	var files []string
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
		files = mapFiles[:ntasks]
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
		files = mapFiles[:ntasks]
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
  var wg sync.WaitGroup
  var jobMng JobManager
  jobMng.Init(files)
  workerInfo := make(map[string]*WorkerInfo)
  scheduleChan := make(chan TaskRsp)
	
finish:
  for {

  selectRegister:
    for{
    select {
      case worker :=<-registerChan:
				fmt.Println("Get Worker Register", worker, "Phase", phase)
        workerChan := make(chan TaskReq)
        workerInfo[worker] = &WorkerInfo{workerChan, false} 
        wg.Add(1)
        go func(worker string, workerChan chan TaskReq){

					fmt.Println("Start Worker", worker)

          defer func(){
						wg.Done()
						fmt.Println("End Worker", worker)
					}()

          outer:
          for {
            select {
              case task := <-workerChan:
								//fmt.Printf("Worker:%s, GetTask: end:%v, file:%s, index:%d\n", worker, task.end, task.file, task.index)
                if task.end {
                  //taskRsp := TaskRsp{1, task.file, worker} 
                  //scheduleChan <- taskRsp
                  break outer 
                }else{
                  args := DoTaskArgs{jobName, task.file, phase,task.index, n_other}
                  ret := call(worker,"Worker.DoTask",args,&args)
									//fmt.Printf("Call Ret, %v\n", ret)
                  if ret {
										//fmt.Printf("Rask file:%s, worker%s Success!!\n", task.file, worker)
										taskRsp := TaskRsp{0, task.file, worker} 
										scheduleChan <- taskRsp
                  }else{
										fmt.Printf("Rask file:%s, worker%s Failed!!\n", task.file, worker)
                    taskRsp := TaskRsp{1, task.file, worker} 
                    scheduleChan <- taskRsp
                    break outer
									}
                }
              default:
            }
          }
					fmt.Printf("worker:%s, exit!!!!!!!!!!!!!\n", worker)
        }(worker, workerChan)
        default:
          break selectRegister
      }
    }
  selectFinishJob:
    for{
      select{
        case jobInfo := <-scheduleChan:
          if jobInfo.status == 1 {
						fmt.Printf("Get Failed JobInfo, file:%s, worker:%s, status:%d\n", jobInfo.file, jobInfo.worker, jobInfo.status)
            jobMng.ResetJob(jobInfo.file, jobInfo.worker)
            delete(workerInfo, jobInfo.worker) 
          }else{
            jobMng.SetFinished(jobInfo.file, jobInfo.worker) 
						workerInfo[jobInfo.worker].busy = false
          }
        default:
        break selectFinishJob
      }
    }
//		fmt.Printf("Finish JobNumber:%d\n", jobMng.GetFinishJobNumber())
		job, allDone := jobMng.GetJob()
		if job == nil && !allDone {
			// no job, wait until work is done	
		}else if job != nil {
			for worker, info := range workerInfo {
				if !info.busy{
					jobMng.SetStart(job.file, worker)
					info.busy = true
					info.workchan <- TaskReq{false, job.file, job.index}
					break
				}
			}
		}else if (allDone){
			fmt.Printf("All Done!!!!!\n")
			for _,info := range workerInfo {
					info.workchan <- TaskReq{true, "", 0}
			}
			break finish
		}
  }

  wg.Wait()
	fmt.Printf("Schedule: %v phase done\n", phase)
}
