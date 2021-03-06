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

	"bytes"
	"encoding/gob"
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

	//nextIndexMu sync.Mutex
	lastApplied int
	nextIndex []int
	matchIndex []int
	role int
	log []LogInfo
	applyCh chan ApplyMsg
	commitReplyCh chan ApplyMsg
	commitIndex int
	haveLogApplied bool
	killed bool
}


func (rf *Raft)String()string{
	return fmt.Sprintf("RaftState: Raft:%d, currentTerm:%d,isLeader:%v, voteFor:%d,Role:%d,commitIndex:%d, apply:%d\n" +
		"log:len:%d, lastIndex:%d, lastTerm:%d, nextindex:%v, matchindex:%v\n",
	rf.me, rf.currentTerm,rf.IsLeader(), rf.voteFor, rf.role, rf.commitIndex,
		rf.lastApplied, len(rf.log), rf.GetLastLogIndex(), rf.GetLastLogTerm(), rf.nextIndex, rf.matchIndex)
}
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return rf.currentTerm,  rf.role==ROLE_LEADER
}

func (rf *Raft)GetLastLogIndex()int{
	return len(rf.log)
}

func (rf *Raft)GetLastLogTerm()int{
	if len(rf.log) > 0{
		return rf.log[len(rf.log)-1].Term
	}else{
		return 0
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
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)
	e.Encode(rf.lastApplied)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	 r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.voteFor)
	d.Decode(&rf.log)
	d.Decode(&rf.lastApplied)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	fmt.Printf("Raft:%d, currentTerm:%d, voteFor:%d, lastApplied:%d\n", rf.me, rf.currentTerm, rf.voteFor, rf.lastApplied)
}
func (rf *Raft)IncTerm()int{
	rf.currentTerm += 1
	return rf.currentTerm
}

func (rf *Raft)VoteFor(id int){
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
	fmt.Printf("Raft:%d, Add Log, len:%d\n", rf.me, len(rf.log))
}
func (rf* Raft)GetCommitIndex()int{
	return rf.commitIndex
}
func (rf* Raft)SendAppendLogRpc(i int, req *RequestAppendLog, rsp *RequestAppendLogReply) bool {
	ok := rf.peers[i].Call("Raft.RequestAppendLog",req, rsp)
	return ok
}

func (rf *Raft)SetCommitIndex( value int){
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	if rf.commitIndex < value{
		rf.commitIndex = value
	}
}
func (rf *Raft)GetPreLogTermAndIndex(index int) (int, int, int){
	nextIndex :=  rf.nextIndex[index]
	lastSendLogIndex := nextIndex -1
	if lastSendLogIndex > len(rf.log){
		panic(fmt.Sprintf("Raft:%d, Peer:%d, logLen:%d, index:%d\n",
		rf.me, index, len(rf.log), lastSendLogIndex))
	}
	if lastSendLogIndex > 0{
		return rf.GetLogByIndex(lastSendLogIndex).Term, lastSendLogIndex,nextIndex
	}
	return 0,0,1
}
type CommitResult struct{
	peerid int
	result bool
	preLogIndex int
	logNum int
	reply RequestAppendLogReply
}
func (rf *Raft)CommitLog(cmd interface{}) (index int, term int){
	rf.mu.Lock()
	rf.AddLocalLog(cmd)
	rf.matchIndex[rf.me]=rf.GetLastLogIndex()
	logIndex := rf.GetLastLogIndex()
	logTerm := rf.GetLastLogTerm()
	rf.mu.Unlock()

	go func(){
		rspCh := make(chan CommitResult)

		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(peerid int){
				rf.mu.Lock()

				prefLogTerm,preLogIndex, nextIndex :=rf.GetPreLogTermAndIndex(peerid)
				sendLogs := rf.GetSendLog(peerid,nextIndex,true)
				logNum := len(sendLogs)
				req := RequestAppendLog{rf.currentTerm,rf.me,preLogIndex,
					prefLogTerm,rf.GetCommitIndex(), logNum,sendLogs}
				reply := RequestAppendLogReply{}
			 	oldTerm := rf.currentTerm

				rf.mu.Unlock()

				ok :=rf.SendAppendLogRpc(peerid,&req, &reply)

				if oldTerm == rf.currentTerm {
					fmt.Printf("Leader:%d, Append Rsp from:%d, logindex:%d, rpcok:%v," +
						"result:%v,preIndex:%d, logNum:%d\n",
						rf.me, peerid, logIndex, ok, reply,preLogIndex, logNum)
				}
				if ok && reply.Success {
					rspCh <- CommitResult{peerid, true,
						preLogIndex, logNum, reply}
				}else{
					rspCh <- CommitResult{peerid, false,
						preLogIndex, logNum, reply}
				}

			}(i)
		}
		replyNum := 1
		commitNum :=1
		End:
		for{
			select {
			case ret :=<- rspCh:
				rf.mu.Lock()
				if ret.reply.Term > rf.currentTerm {
					rf.currentTerm = ret.reply.Term
					rf.ChangeToFollower()
					rf.voteFor = -1
				}

				if !rf.IsLeader() {
					rf.mu.Unlock()
					break End
				}

				replyNum += 1
				if ret.result {
					if  rf.nextIndex[ret.peerid] < ret.preLogIndex+ret.logNum+1 {
						rf.nextIndex[ret.peerid] = ret.preLogIndex+ret.logNum+1
						rf.matchIndex[ret.peerid] = ret.preLogIndex+ret.logNum
					}

					rf.HandleCommitIndex()
				}else{
					if rf.nextIndex[ret.peerid] > 1{
						rf.nextIndex[ret.peerid] -=1
						fmt.Printf("%v, Raft:%d, Decret Peer :%d, nextIndex;%d\n",
							time.Now().Unix(),rf.me, ret.peerid,rf.nextIndex[ret.peerid])
					}
				}
				if replyNum == len(rf.peers){
					fmt.Printf("All Rsp Received, raft:%d, isLeader:%v,commit index:%d," +
						" new index:%d.Commit Peer Num:%d,\n",
						rf.me, rf.IsLeader(),  rf.commitIndex, logIndex, commitNum)

					rf.mu.Unlock()
					break End
				}
				rf.mu.Unlock()
			}
		}
	}()
	fmt.Printf("Raft:%d, Start Index:%d, Term:%d Value:%v\n", rf.me, logIndex, logTerm,cmd)
	return logIndex,logTerm
}
func (rf *Raft) HandleCommitIndex() {
	oldIndex := rf.commitIndex
	//if commitNum > len(rf.peers)/2 && rf.IsLeader()&&oldIndex < logIndex {
	haveNewCommit, newCommitIndex := rf.haveNewCommitedLog()
	if haveNewCommit {
		logNum := newCommitIndex - oldIndex
		for i := 1; i <= logNum; i++ {
			commitIndex := oldIndex+i
			if rf.lastApplied < commitIndex {
				fmt.Printf("Raft:%d, Commit Log:%d,cmd%d\n", rf.me,
					commitIndex, rf.GetLogByIndex(commitIndex).Value)

				rf.SetCommitIndex(commitIndex)
				msg := ApplyMsg{}
				msg.Command = rf.GetLogByIndex(commitIndex).Value
				msg.Index = commitIndex
				rf.applyCh <- msg

				rf.lastApplied = commitIndex
				rf.persist()
			}
		}
		rf.commitIndex = newCommitIndex
	}
}
func (rf *Raft) haveNewCommitedLog() (bool, int){
	oldCommitIndex := rf.commitIndex
	peerNum := len(rf.peers)
	newCommitIndex := oldCommitIndex
	for i := oldCommitIndex+1; i <= rf.GetLastLogIndex(); i++{
		matchNum := 1
		for index, matchIndex := range rf.matchIndex {
			if index != rf.me && matchIndex >= i {
				matchNum++
				if matchIndex > rf.GetLastLogIndex() {
					panic(fmt.Sprintf("match index illegal, rf:%d, index:%d, matchIndex:%d, max:%d\n",
					rf.me, index, matchIndex, rf.GetLastLogIndex()))
				}
			}
		}
		if matchNum > peerNum/2 {
			newCommitIndex = i
		}else{
			break
		}
	}
	if newCommitIndex != oldCommitIndex{
		return true, newCommitIndex
	}
	return false,oldCommitIndex
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
	return fmt.Sprintf("Term:%d, Candidate:%d, logIndex:%d,logTerm:%d",
		req.Term, req.CandidateId,req.LastLogIndex, req.LastLogTerm)
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
func (rf *Raft) LogNotNewer(otherTerm int, otherIndex int) bool{
	myLastLogTerm := rf.GetLastLogTerm()
	if myLastLogTerm < otherTerm {
		return true
	}
	myLastLogIndex := rf.GetLastLogIndex()
	if myLastLogTerm == otherTerm &&  myLastLogIndex <= otherIndex {
		return true
	}
	return false
}
//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//fmt.Printf("Raft:%d, Get RequestVote Arg:%v\n", rf.me, args)
	rf.mu.Lock()
	fmt.Printf("Raft:%d,Get ReqVote Arg Candidate:%d, term:%d,log index:%d, logterm:%d\n",
		rf.me, args.CandidateId, args.Term,args.LastLogIndex, args.LastLogTerm)
	fmt.Printf("Raft:%d, Role:%d, voteFor:%d, Term:%d, log index:%d, log term:%d\n",
		rf.me, rf.role, rf.voteFor, rf.currentTerm, rf.GetLastLogIndex(), rf.GetLastLogTerm())
	if args.Term < rf.currentTerm {
		fmt.Printf("Raft:%d, Grant Failed, MyTerm:%d, Req:%d\n",rf.me, rf.currentTerm, args.Term)
		reply.VoteGranted = false;
	}else if args.Term > rf.currentTerm{
		rf.currentTerm = args.Term
		rf.ChangeToFollower()
		if rf.LogNotNewer(args.LastLogTerm, args.LastLogIndex) {
			rf.VoteFor(args.CandidateId)
			reply.VoteGranted = true
		}else{
			rf.VoteFor(-1)
			reply.VoteGranted = false
		}
		fmt.Printf("Raft :%d, Change To Follower, Term:%d, VoteFor:%d, \n", rf.me, rf.currentTerm, rf.voteFor)

	}else if (rf.voteFor == -1 || rf.voteFor == args.CandidateId) && rf.LogNotNewer(args.LastLogTerm, args.LastLogIndex) {
			rf.currentTerm = args.Term
			reply.VoteGranted = true
			rf.VoteFor(args.CandidateId)
			rf.ChangeToFollower()
			fmt.Printf("Raft :%d, Grant To:%d\n", rf.me, args.CandidateId)

	}else{
		fmt.Printf("Raft:%d, Grant Failed\n",rf.me)
		reply.VoteGranted = false
	}

	reply.Term=rf.currentTerm
	rf.mu.Unlock()
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
	if index == 0 && term == 0 {
		return true
	}

	if len(rf.log) < index{
		return false
	}
	return rf.log[index-1].Term == term
}
func (rf *Raft)ConflictLog(index int, term int) bool{
	if len(rf.log) >= index && rf.GetLogByIndex(index).Term != term{
		return true
	}
	return false
}
func (rf *Raft)ElimitConflict(index int){
	endIndex := index-1
	if endIndex < len(rf.log) {
		rf.log = rf.log[:endIndex]
	}
}
func (rf *Raft)AddLog(index int, term int, log LogInfo){
	if rf.ConflictLog(index, term) {
		fmt.Printf("Raft:%d,Have Conflict,index:%d, term:%d,value:%v\n",rf.me, index, term, log)
		rf.ElimitConflict(index)
		rf.AddLog(index, term, log)
	}
	if rf.ContainLog(index, term) {
		fmt.Printf("Raft:%d, Alread Contain　Log:Index:%d, term:%d\n", rf.me, index, term)
		return
	}
	if len(rf.log) >=index {
		fmt.Printf("Raft:%d Add Log ByIndex:%d,value:%v\n",rf.me, index, log)
		rf.log[index-1] = log
	} else {
		fmt.Printf("Raft:%d Add Log By Append, Index:%d, value:%v\n",rf.me,index, log)
		rf.log=append(rf.log,log)
	}
	rf.persist()
}
func min (a int, b int)int{
	if a < b {
		return a
	}
	return b
}
func (rf *Raft) RequestAppendLog(args *RequestAppendLog, reply *RequestAppendLogReply) {
	rf.mu.Lock()
	fmt.Printf("Raft:%d,comitIndex:%d, term:%d, AppendLog,leader:%d,arg:%v\n",
		rf.me,rf.commitIndex, rf.currentTerm, rf.voteFor,args)

	if args.Term >= rf.currentTerm && args.LeaderId == rf.voteFor  {
		rf.ResetElectionTimeOut()
		if rf.ContainLog(args.PrevLogIndex, args.PrevLogTerm) {
			fmt.Printf("time:%v,Raft:%d, Contain Log prevIndex:%d, term:%d\n",
				time.Now().Unix(),rf.me, args.PrevLogIndex,args.PrevLogTerm)

			reply.Term = args.Term
			reply.Success = true
			rf.currentTerm = args.Term

			for index :=0; index < args.LogNum; index++{
				logIndex := args.PrevLogIndex+index+1
				logTerm := args.AppendLog[index].Term
				rf.AddLog(logIndex, logTerm, args.AppendLog[index])
			}
			raftCommitIndex := rf.commitIndex
			if args.LeaderCommit > raftCommitIndex{
				oldCommitIndex := raftCommitIndex
				newCommitIndex := min(args.LeaderCommit, rf.GetLastLogIndex())
				rf.SetCommitIndex(newCommitIndex)
				for i :=1; i <= newCommitIndex- oldCommitIndex;i++{
					msg:=ApplyMsg{oldCommitIndex+i,rf.GetLogByIndex(oldCommitIndex+i).Value,false,[]byte{}}
					fmt.Printf("Raft:%d, ApplyMsg:index:%d, command;%v\n", rf.me, oldCommitIndex+i, rf.GetLogByIndex(oldCommitIndex+i))
					rf.applyCh <- msg
				}
			}
		}else {
			//rf.ElimitConflict(args.PrevLogIndex)
			fmt.Printf("Raft:%d, no preLog prevIndex:%d, term:%d\n",rf.me, args.PrevLogIndex,args.PrevLogTerm)
			reply.Success = false
		}
	}else{
		if args.Term > rf.currentTerm {
			rf.ChangeToFollower()
			rf.currentTerm = args.Term
			if rf.IsCandidate() {
				rf.voteFor = args.LeaderId
			}else{
				rf.voteFor = -1
			}
			rf.ResetElectionTimeOut()
		}
		reply.Term = rf.currentTerm
		reply.Success = false
	}
	fmt.Printf("Raft:%d, AppendLogResult%v, CommitIndex:%d\n",rf.me, reply, rf.commitIndex)
	rf.mu.Unlock()
	return
}
func (rf *Raft) ChangeToCandidate(){
	rf.role = ROLE_CANDIDATE

}
func (rf *Raft)IsLeader()bool{
	return rf.role == ROLE_LEADER
}
func (rf *Raft)IsCandidate()bool{
	return rf.role == ROLE_CANDIDATE
}
func (rf *Raft)print(fstr string, args ...interface{} ){
	if !rf.killed {
		fmt.Printf("Raft:%v:"+fstr, rf, args)
	}
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
	rf.electionTimeOutMs = time.Millisecond*(time.Duration(rand.Int31n(ELECTION_RANGE) + ELECTION_BASE))
	//fmt.Printf("Rf:%d, time after :%v\n", rf.me, rf.electionTimeOutMs)
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
	if isLeader {
		index, term = rf.CommitLog(command)
	}
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.killed = true
}
func (rf *Raft) SelectFailed(term int) {
	// Your code here, if desired.
	if rf.currentTerm == term {
		fmt.Printf("Raft:%d, Election Term:%d, Failed\n", rf.me, rf.currentTerm)
		rf.voteFor = -1
	}
}
func (rf *Raft)GetLogByIndex(index int)LogInfo{
	if len(rf.log)  < index {
		panic(fmt.Sprintf("raft:%d, index outofrange len:%d, index:%d\n", rf.me, len(rf.log), index))
	}
	if index <= 0{
		panic(fmt.Sprintf("Raft:%d, Index Value:%d\n", rf.me, index))
	}
	return rf.log[index-1]
}
func (rf* Raft)GetSendLog(peerid int, peerNextIndex int, commit bool)[]LogInfo{
	lastLogIndex := rf.GetLastLogIndex()

	logNum := lastLogIndex - peerNextIndex + 1
	if logNum <= 0 {
		return  make([]LogInfo, 0,0)
	}

	logs := make([]LogInfo, logNum,logNum)

	if logNum > 0 {
		//fmt.Printf("Raft:%d, SendTo:%d, Start\n", rf.me, peerid)
		for i := 0; i< logNum; i++{
			//fmt.Printf("Raft:%d, SendTo:%d, index:%d,len:%d\n", rf.me, peerid, peerNextIndex+i, len(rf.log))
			log := rf.GetLogByIndex(peerNextIndex+i)
			logs[i] = log
		}
		//fmt.Printf("Raft:%d, SendTo:%d, End\n", rf.me, peerid)
	}
	return logs
}

func (rf *Raft)DecretNextIndex(peerid int) {
	index := rf.nextIndex[peerid]
	if index > 1 {
		rf.nextIndex[peerid] = index -1
	}else{
		rf.nextIndex[peerid] = 1
	}
	fmt.Printf("Raft:%d, Set Peer:%d, NextIndex:%d\n", rf.me, peerid, rf.nextIndex[peerid])
}
func SayHello (rf *Raft, peerid int){
	rf.mu.Lock()
	p := rf.peers[peerid]

	prefLogTerm,preLogIndex,nextIndex :=rf.GetPreLogTermAndIndex(peerid)

	sendLogs := rf.GetSendLog(peerid, nextIndex, false)

	req := RequestAppendLog{rf.currentTerm,rf.me,
		preLogIndex,prefLogTerm,
		rf.commitIndex,len(sendLogs),sendLogs}
	reply := RequestAppendLogReply{}

	rf.mu.Unlock()

	p.Call("Raft.RequestAppendLog",&req,  &reply)

	rf.mu.Lock()
	if rf.killed {
		return
	}
	if reply.Term > rf.currentTerm{
		rf.ChangeToFollower()
		rf.currentTerm = reply.Term
		rf.voteFor = -1
	}else if rf.IsLeader(){
		if !reply.Success {
			if rf.nextIndex[peerid] > 1 {
				rf.DecretNextIndex(peerid)
			}
			fmt.Printf("%v:Raft:%d, DecretPeer:%d, nextIndex:%d\n", time.Now().Unix(), rf.me, peerid, rf.nextIndex[peerid])
		}else{
			nextIndex := preLogIndex + len(sendLogs)+1
			rf.matchIndex[peerid] = nextIndex - 1
			rf.nextIndex[peerid] = nextIndex
			rf.HandleCommitIndex()
		}
	}
	rf.mu.Unlock()
}
func HeartBeatGoroutine(rf *Raft){
	End:
	for{
		tick := time.Tick(100*time.Millisecond)
		select {
		case <-tick:
			if rf.killed {
				break End
			}
			for i := range rf.peers {
				if !rf.IsLeader(){
					break End
				}
				if i != rf.me {
					go SayHello(rf, i)
				}
			}
		}
	}
	fmt.Printf("Raft:%d, End  Say Hello\n", rf.me)
}
func (rf *Raft) IamNewLeader() {
	// Your code here, if desired.
	//rf.ResetElectionTimeOut()

	rf.ChangeToLeader()
	go HeartBeatGoroutine(rf)
}
func (rf *Raft)SetNextIndex(peerid int, index int){
	rf.nextIndex[peerid] = index
}
func (rf *Raft)ChangeToLeader(){
	if rf.IsCandidate() {
		fmt.Printf("Raft:%d Is Leader Now!\n", rf.me)
		rf.role = ROLE_LEADER
		for i := range rf.nextIndex{
			rf.SetNextIndex(i, rf.GetLastLogIndex()+1)
		}
		rf.matchIndex = make([]int,len(rf.peers))
		rf.commitIndex = 0
		rf.voteFor = rf.me
	}
}
func (rf *Raft)ChangeToFollower(){
	rf.role = ROLE_FOLLOWER
	rf.ResetElectionTimeOut()
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
			if raft.killed {
				return
			}
			fmt.Printf("Request Vote Rpc  Raft:%d, Ret:%v,req:%v reply:%v\n", index, suc, req, reply)
			if suc {
				if reply.Term == term  && reply.VoteGranted {
					electChan  <- true
				}else{
					if reply.Term > raft.currentTerm{
						raft.currentTerm = reply.Term
						raft.ChangeToFollower()
						raft.voteFor = -1
					}
					electChan <-false
				}
			}else{
				electChan <- false
			}
		}(p, i)
	}
	rspNum :=1
	grantedNum := 1
	ElectResult:
	for {
		select {
		case result := <-electChan:
			rspNum += 1
			raft.mu.Lock()
			if raft.killed {
				raft.mu.Unlock()
				break ElectResult
			}
			if result {
				grantedNum += 1
				if grantedNum > len(raft.peers)/2 && raft.IsCandidate(){
					raft.IamNewLeader()
				}
			}
			if rspNum == len(raft.peers) {
				 if grantedNum <= len(raft.peers)/2 {
					raft.SelectFailed(term)
				}
				raft.mu.Unlock()
				break ElectResult
			}
			raft.mu.Unlock()
		}
	}
}
func StartElection(raft *Raft){
	raft.ChangeToCandidate()
	raft.ResetElectionTimeOut()
	nowTerm := raft.IncTerm()
	fmt.Printf("%v: Election Time Out, Raft:%d, Start Term:%d, Election\n",time.Now().Unix(), raft.me,nowTerm)
	raft.VoteFor(raft.me)
	go NewElection(raft, nowTerm)
}
func HandleElectionTime(raft *Raft){
	End:
	for {
		// Just Started Server, Folloer State, Start Election TimeOut
		gap:=time.Millisecond*10
		tick := time.Tick(gap)
		select {
		case <- tick:
			if raft.IsLeader() {
				continue
			}
			if raft.killed{
				break End
			}
			raft.electionTimeOutMs -= gap
			// timeout for selection
			if raft.electionTimeOutMs <= gap{
				raft.mu.Lock()
				StartElection(raft)
				raft.mu.Unlock()
			}
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
	rf.currentTerm = 0
	rf.role = ROLE_FOLLOWER
	rf.ResetElectionTimeOut()
	rf.applyCh = applyCh

	rf.SetCommitIndex(0)
	rf.nextIndex = make([]int,len(rf.peers))
	rf.lastApplied = 0
	//rf.haveLogApplied = false
	rf.killed = false
	// Your initialization code here (2A, 2B, 2C).
	go HandleElectionTime(rf)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
