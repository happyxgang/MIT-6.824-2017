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
  jobMng.jobInfo[file].worker = file
  jobMng.jobInfo[file].status = 1
}
func (jobMng *JobManager)SetFinished(file string, worker string){
  if jobMng.jobInfo[file].worker == worker {
    jobMng.jobInfo[file].status = 2
  }
}
func (jobMng *JobManager)ResetJob(file string, worker string){
  if jobMng.jobInfo[file].worker == worker {
    jobMng.jobInfo[file].status = 0
  }
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

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
  var wg sync.WaitGroup
  var jobMng JobManager
  jobMng.Init(mapFiles)
  workerInfo := make(map[string] chan TaskReq)
  scheduleChan := make(chan TaskRsp)
finish:
  for {

  selectRegister:
    for{
    select {
      case worker :=<-registerChan:
        workerChan := make(chan TaskReq)
        workerInfo[worker] = workerChan
        wg.Add(1)
        go func(worker string){
          defer wg.Done()
          outer:
          for {
            select {
              case task := <-workerChan:
                if task.end {
                  taskRsp := TaskRsp{1, task.file, worker} 
                  scheduleChan <- taskRsp
                  break outer
                }else{
                  args := DoTaskArgs{jobName, task.file, phase,task.index, n_other}
                  ret := call(worker,"Worker.DoTask",args,&args)
                  if ret {
                    taskRsp := TaskRsp{1, task.file, worker} 
                    scheduleChan <- taskRsp
                    break outer
                  }
                  taskRsp := TaskRsp{0, task.file, worker} 
                  scheduleChan <- taskRsp
                }
              default:
            }
          }
        }(worker)
        default:
          break selectRegister
      }
    }
  selectFinishJob:
    for{
      select{
        case jobInfo := <-scheduleChan:
          if jobInfo.status == 1 {
            jobMng.ResetJob(jobInfo.file, jobInfo.worker)
            delete(workerInfo, jobInfo.worker) 
          }else{
            jobMng.SetFinished(jobInfo.file, jobInfo.worker) 
          }
          job, allDone := jobMng.GetJob()
          if job == nil && !allDone {
            
          }else if job != nil{
            workerInfo[jobInfo.worker] <- TaskReq{false, job.file, job.index}
          }else if (allDone){
            for _,v := range workerInfo {
                v <- TaskReq{true, "", 0}
            }
            break finish
          }
        default:
        break selectFinishJob
      }
    }
  }
  wg.Wait()
	fmt.Printf("Schedule: %v phase done\n", phase)
}
