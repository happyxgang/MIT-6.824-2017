package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"sync"
	"labrpc"
	"time"
	"fmt"
	"math/rand"

)

// import "bytes"
// import "encoding/gob"


const ELECTION_BASE = 500
const ELECTION_RANGE = 250

const(
	ROLE_FOLLOWER = iota
	ROLE_CANDIDATE = iota
	ROLE_LEADER = iota
)
//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}
type LogInfo struct{
	Value interface{}
	Term int
}
//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	electionTimeOutMs time.Duration
	currentTerm int
	voteFor int 
	nextIndex []int
	matchIndex []int
	role int
	log []LogInfo
	applyCh chan ApplyMsg
	commitReplyCh chan ApplyMsg
	commitIndex int
}
func (rf *Raft)String()string{
	return fmt.Sprintf("RaftState:currentTerm:%d, voteFor:%d,Role:%d,commitIndex:%d\n",
	rf.currentTerm, rf.voteFor, rf.role, rf.commitIndex)
}
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return rf.currentTerm,  rf.role==ROLE_LEADER
}

func (rf *Raft)GetLastLogIndex()int{
	return len(rf.log)-1
}

func (rf *Raft)GetLastLogTerm()int{
	if len(rf.log) > 0{

		return rf.log[len(rf.log)-1].Term
	}else{
		return -1
	}
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}
func (rf *Raft)IncTerm()int{
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm += 1
	return rf.currentTerm
}

func (rf *Raft)VoteFor(id int){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.voteFor = id
	rf.ResetElectionTimeOut()
}

func (rf *Raft)GetLastLog()*LogInfo{
	return rf.GetLastIndexLog(0)
}
func (rf *Raft)GetLastIndexLog(count int) *LogInfo{
	index := len(rf.log)-1 + count
	if index >= 0 {
		return &rf.log[index]
	}
	return nil
}

func (rf* Raft)AddLocalLog(cmd interface{}){
	rf.log = append(rf.log, LogInfo{cmd, rf.currentTerm})
}
func (rf* Raft)GetCommitIndex()int{
	return rf.commitIndex
}
func (rf* Raft)SendAppendLogRpc(i int, req *RequestAppendLog, rsp *RequestAppendLogReply) bool {
	ok := rf.peers[i].Call("Raft.RequestAppendLog",req, rsp)
	return ok
}
func (rf *Raft)CommitLog(cmd interface{}) (index int, term int){
	preLogIndex :=rf.GetLastLogIndex()
	prefLogTerm :=rf.GetLastLogTerm()

	rf.AddLocalLog(cmd)
	rspCh := make(chan bool)
	logIndex:=rf.GetLastLogIndex()
	logTerm:=rf.GetLastLogTerm()
	lastlog := rf.GetLastLog()
	appendLog :=[]LogInfo{*lastlog}
	go func(){
		req := RequestAppendLog{rf.currentTerm,rf.me,preLogIndex,prefLogTerm,rf.GetCommitIndex(), 1,appendLog}
		reply := RequestAppendLogReply{}
		for i, _ := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(index int){
			 	ok :=rf.SendAppendLogRpc(index,&req, &reply)
				fmt.Printf("Append Rsp:index:%d me:%d, other:%d, ok:%v\n",logIndex, rf.me, index, ok)
				if ok && reply.Success {
					rspCh <- true
				}else{
					rspCh <- false
				}
			}(i)
		}
		replyNum := 1
		commitNum :=1
		End:
		for{
			select {
			case result :=<- rspCh:
				replyNum += 1
				if result {
					commitNum +=1
				}
				if replyNum == len(rf.peers){
					fmt.Printf("All Rsp Received, raft:%d, isLeader:%v,CommitNum:%d, commit index;%d, new index:%d\n",
						rf.me, rf.IsLeader(), commitNum, rf.commitIndex, logIndex)
					if commitNum > len(rf.peers)/2 {
						if rf.commitIndex < logIndex {
							if rf.IsLeader(){
								fmt.Printf("Raft:%d, Commit Log:%d,cmd%d\n",rf.me, logIndex,cmd)
								rf.commitIndex=logIndex
								msg := ApplyMsg{}
								msg.Command = cmd
								msg.Index = logIndex+1
								rf.applyCh <- msg
							}else{
								err:=fmt.Sprintf("rf:%d, not Leader\n", rf.me)
								panic(err)
							}
						} else{
							panic("CommitIndex roll back?!")
						}
					}
					break End
				}
			}
		}
	}()
	fmt.Printf("Raft:%d, Start Index:%d, Term:%d\n", rf.me, logIndex, logTerm)
	return logIndex,logTerm
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
		Term         int
		CandidateId  int
		LastLogIndex int
		LastLogTerm  int
}

func (req RequestVoteArgs) String()string{
	return fmt.Sprintf("Term:%d, Candidate:%d, logIndex:%d,logTerm:%d", req.Term, req.CandidateId,req.LastLogIndex, req.LastLogTerm)
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
		Term        int
		VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	fmt.Printf("Raft:%d, Get RequestVote Arg:%v\n", rf.me, args)
	fmt.Printf("Raft:%d, ReqVoteArg Candidate:%d, term:%d,log index:%d, logterm:%d\n", rf.me, args.CandidateId, args.Term,args.LastLogIndex, args.LastLogTerm)
	fmt.Printf("me:%d, voteFor:%d, Term:%d, log index:%d, log term:%d\n", rf.me, rf.voteFor, rf.currentTerm, rf.GetLastLogIndex(), rf.GetLastLogTerm())
	if args.Term < rf.currentTerm {
		fmt.Printf("Raft:%d, Grant Failed, raft term:%d, arg term%:d\n",rf.me, rf.currentTerm, args.Term)
		reply.VoteGranted = false
	}else if (rf.voteFor == -1 || rf.voteFor == args.CandidateId) && (rf.GetLastLogTerm() == args.LastLogTerm && rf.GetLastLogIndex() <= args.LastLogIndex) {
		rf.currentTerm = args.Term
		reply.VoteGranted = true
		rf.VoteFor(args.CandidateId)
		rf.ChangeToFollower()
		fmt.Printf("Raft :%d, Grant To:%d\n", rf.me, args.CandidateId)
		//rf.ResetElectionTimeOut()
	}else{
		fmt.Printf("Raft:%d, Grant Failed\n",rf.me)
		reply.VoteGranted = false
	}
	reply.Term=rf.currentTerm
}
type RequestAppendLog struct{
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	//Entries []LogInfo
	LeaderCommit int
	LogNum int
	AppendLog []LogInfo
}
func (arg *RequestAppendLog)String()string{
	return fmt.Sprintf("AppendLogReq Term:%d,Leader:%d,PreIndex:%d,PreTerm:%d,LeaderCommit:%d,LogNum:%d\n",
	arg.Term,arg.LeaderId,arg.PrevLogIndex,arg.PrevLogTerm,arg.LeaderCommit,arg.LogNum)
}
type RequestAppendLogReply struct{
	Term int
	Success bool
}
func (rf *Raft)ContainLog(index int, term int) bool{
	if index <0 && term<0 {
		return true
	}

	if len(rf.log)-1 < index{
		return false
	}
	return rf.log[index].Term == term
}
func (rf *Raft)ConflictLog(index int, term int) bool{
	if len(rf.log)-1 >= index && rf.log[index].Term != term{
		return true
	}
	return false
}
func (rf *Raft)ElimitConflict(index int){
	endIndex := index
	rf.log = rf.log[:endIndex]
}
func (rf *Raft)AddLog(index int, term int, log LogInfo){
	if rf.ConflictLog(index, term) {
		fmt.Printf("Raft:%d,Have Conflict,index:%d, term:%d,value:%v\n",rf.me, index, term, log)
		rf.ElimitConflict(index)
		rf.AddLog(index, term, log)
	}
	if len(rf.log)-1 >=index{
		fmt.Printf("Raft:%d Add Log ByIndex:%d,value:%v\n",rf.me, index, log)
		rf.log[index] = log
	}else{
		fmt.Printf("Raft:%d Add Log By Append,value:%v\n",rf.me,log)
		rf.log=append(rf.log,log)
	}
}
func min (a int, b int)int{
	if a < b {
		return a
	}
	return b
}
func (rf *Raft) RequestAppendLog(args *RequestAppendLog, reply *RequestAppendLogReply) {
	fmt.Printf("AppendLog,Sender %d, Receier:%d leader:%d,arg:%v\n", args.LeaderId, rf.me,rf.voteFor,args)
	if args.Term >= rf.currentTerm && args.LeaderId == rf.voteFor  {
		if(rf.ContainLog(args.PrevLogIndex, args.PrevLogTerm)){
			fmt.Printf("Contain Log prevIndex:%d, term:%d\n", args.PrevLogIndex,args.PrevLogTerm)

			reply.Term = args.Term
			reply.Success = true
			rf.currentTerm = args.Term
			// TODO::check when to reset election time
			rf.ResetElectionTimeOut()

			for index :=0; index < args.LogNum; index++{
				logIndex := args.PrevLogIndex+1+index
				logTerm := args.Term
				rf.AddLog(logIndex, logTerm, args.AppendLog[index])
			}
			if args.LeaderCommit > rf.commitIndex {
				oldCommitIndex := rf.commitIndex
				rf.commitIndex = min(args.LeaderCommit, rf.GetLastLogIndex())
				for i :=1; i <= rf.commitIndex - oldCommitIndex;i++{
					msg:=ApplyMsg{oldCommitIndex+i+1,rf.log[oldCommitIndex+i].Value,false,[]byte{}}
					rf.applyCh <- msg
				}
			}
		}else {
			fmt.Printf("Not Log prevIndex:%d, term:%d\n", args.PrevLogIndex,args.PrevLogTerm)
			reply.Success = false
		}
	}else{

		if args.Term > rf.currentTerm {
			rf.ChangeToFollower()
			rf.currentTerm = args.Term
		}
		reply.Term = rf.currentTerm
		reply.Success = false
	}
	fmt.Printf("AppendLogResult:%v, Raft:%d, CommitIndex:%d\n",reply,rf.me, rf.commitIndex)
	return
}
func (rf *Raft) ChangeToCandidate(){
	rf.role = ROLE_CANDIDATE
}
func (rf *Raft)IsLeader()bool{
	return rf.role == ROLE_LEADER
}
//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) SendRpc(server int, method string, args *interface{}, reply *interface{}) bool {
	ok := rf.peers[server].Call(method, args, reply)
	return ok
}


func (rf *Raft)ResetElectionTimeOut(){
	rf.electionTimeOutMs = time.Millisecond*(time.Duration((rand.Int31n(ELECTION_RANGE) + ELECTION_BASE)))
	//fmt.Printf("Rf:%d, time after :%v\n", rf.me, rf.electionTimeOutMs)
}



func (rf *Raft)BroadCastReq(rpc string, req interface{}, replyfuc func(replyarr []interface{})){
//	electChan := make(chan bool)
//
//	for i, p := range raft.peers {
//		if i == raft.me {
//			continue
//		}
//
//		go func(p *labrpc.ClientEnd, index int){
//			fmt.Printf("Raft:%d:Start To Call for Vote Peer:%d\n",raft.me, index)
//			req := RequestVoteArgs{term, candidateId, lastLogIndex, lastLogTerm}
//			reply := reflect.Type(replyfuc)
//			suc := raft.sendRequestVote(index, &req, &reply)
//			fmt.Printf("Request Vote Rpc Ret:%v,req:%v reply:%v\n", suc, req, reply)
//			if suc {
//				if reply.Term == term  && reply.VoteGranted {
//					electChan  <- true
//				}else{
//					electChan <-false
//				}
//			}else{
//				electChan <- false
//			}
//		}(p, i)
//	}
//	rspNum :=0
//	grantedNum := 1
//ElectResult:
//	for {
//		select {
//		case result := <- electChan:
//			rspNum += 1
//			if( result){
//				grantedNum += 1
//				if grantedNum > len(raft.peers)/2 {
//					raft.IamNewLeader()
//				}
//			}
//			if rspNum == len(raft.peers)-1 {
//				if grantedNum <= len(raft.peers)/2 {
//					raft.SelectFailed()
//				}
//				break ElectResult
//			}
//		}
//	}
}
//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := rf.IsLeader()

	// Your code here (2B).
	if (isLeader){
		index, term = rf.CommitLog(command)
	}
	return index+1, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}
func (rf *Raft) SelectFailed() {
	// Your code here, if desired.
	fmt.Printf("Raft:%d, Election Term:%d, Failed\n", rf.me, rf.currentTerm)
	rf.voteFor = -1
}
func SayHello (rf *Raft, p *labrpc.ClientEnd){
	//log := make([]LogInfo,1,1)
	req := RequestAppendLog{rf.currentTerm,rf.me,rf.GetLastLogIndex(),rf.GetLastLogTerm(),rf.commitIndex,0,[]LogInfo{}}
	reply := RequestAppendLogReply{}
	p.Call("Raft.RequestAppendLog",&req,  &reply)
	if reply.Term > rf.currentTerm{
		rf.ChangeToFollower()
		rf.currentTerm = reply.Term
	}
}
func HeartBeatGoroutine(rf *Raft){
	for{
		tick := time.Tick(100*time.Millisecond)
		select {
		case <-tick:
			for i, p := range rf.peers {
				if i != rf.me {
					go SayHello(rf, p)
				}

			}
		}
	}
}
func (rf *Raft) IamNewLeader() {
	// Your code here, if desired.
	//rf.ResetElectionTimeOut()
	rf.ChangeToLeader()
	go HeartBeatGoroutine(rf)
}
func (rf *Raft)ChangeToLeader(){
	fmt.Printf("Raft:%d Is Leader Now!\n", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.role = ROLE_LEADER
}
func (rf *Raft)ChangeToFollower(){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.role = ROLE_FOLLOWER
}
//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func NewElection(raft *Raft, term int){
	electChan := make(chan bool)

	for i, p := range raft.peers {
		if i == raft.me {
			continue
		}
		candidateId := raft.me

		lastLogIndex := raft.GetLastLogIndex()
		lastLogTerm := raft.GetLastLogTerm()

		go func(p *labrpc.ClientEnd, index int){
			fmt.Printf("Raft:%d:Start To Call for Vote Peer:%d\n",raft.me, index)
			req := RequestVoteArgs{term, candidateId, lastLogIndex, lastLogTerm}
			reply := RequestVoteReply{}
			suc := raft.sendRequestVote(index, &req, &reply)
			fmt.Printf("Request Vote Rpc Ret:%v,req:%v reply:%v\n", suc, req, reply)
			if suc {
				if reply.Term == term  && reply.VoteGranted {
					electChan  <- true
				}else{
					electChan <-false
				}
			}else{
					electChan <- false
			}
		}(p, i)
	}
	rspNum :=0
	grantedNum := 1
	ElectResult:
	for {
		select {
		case result := <- electChan:
			rspNum += 1
			if( result){
				grantedNum += 1

			}
			if rspNum == len(raft.peers)-1 {
				if grantedNum > len(raft.peers)/2 {
					raft.IamNewLeader()
				}else if grantedNum <= len(raft.peers)/2 {
					raft.SelectFailed()
				}
				break ElectResult
			}
		}
	}
}
func StartElection(raft *Raft){
	fmt.Printf("Election Time Out, %d, Start New Election\n", raft.me)

	raft.ChangeToCandidate()
	nowTerm := raft.IncTerm()

	raft.VoteFor(raft.me)

	go NewElection(raft, nowTerm)
}
func HandleElectionTime(raft *Raft){
	for {
		// Just Started Server, Folloer State, Start Election TimeOut
		gap:=time.Millisecond*10
		tick := time.Tick(gap)
		select {
		case <- tick:
				if raft.IsLeader() {
					continue
				}
				raft.electionTimeOutMs -= gap
				// timeout for selection
				if raft.electionTimeOutMs <= gap{
					//ChangeToCandidate(raft)
					raft.ChangeToCandidate()
					raft.ResetElectionTimeOut()
					StartElection(raft)
				}
		}
	}
}


func SendApplyMsg(rf *Raft){
	for{
		select{
		case msg := <- rf.commitReplyCh:
			rf.applyCh <- msg
		}
	}
}
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.voteFor = -1
	rf.currentTerm = -1
	rf.role = ROLE_FOLLOWER
	rf.ResetElectionTimeOut()
	rf.applyCh = applyCh
	rf.commitReplyCh = make(chan ApplyMsg)
	rf.commitIndex = -1
	// Your initialization code here (2A, 2B, 2C).
	go HandleElectionTime(rf)
	go SendApplyMsg(rf)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
