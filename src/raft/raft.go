package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import (
	"labrpc"
	"time"
	"math/rand"
	"fmt"
)

// import "bytes"
// import "encoding/gob"



const (
	LEADER = iota
	CANDIDATE
	FOLLOWER

	HBINTERVERL = 50 * time.Millisecond

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


// Each log entry structure
type LogEntry struct {
	Index 		int
	Command 	interface{}
	Term		int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//Persistent on all servers
	currentTerm	int
	votedFor	int
	log		[]LogEntry


	//Volatile state on all servers
	commitIndex	int
	lastApplied	int


	//Volatile state on leaders
	nextIndex	[]int
	matchIndex	[]int


	//others
	state		int
	voteCount	int
	chanHeartBeat	chan bool
	chanCommit	chan bool
	chanGrantVote	chan bool
	chanLeader	chan bool
	chanApplyMsg	chan ApplyMsg

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here.
	term := rf.currentTerm
	isleader := rf.state == LEADER
	return term, isleader
}



func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}


func (rf *Raft) getLastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}


func (rf *Raft) getPrevLogIndex() int{
	return len(rf.log) -1
}


func (rf *Raft) getPrevLogTerm() int{
	return rf.log[len(rf.log)-1].Term
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
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
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}




//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term		int
	CandidateId 	int
	LastLogIndex	int
	LastLogTerm	int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term	 	int
	VoteGranted	bool
}



//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	if(rf.currentTerm > args.Term){
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}else if(rf.currentTerm == args.Term) {

		if(rf.votedFor == args.CandidateId && args.LastLogIndex >= rf.getLastLogIndex() && args.LastLogTerm >= rf.getLastLogTerm()){
			fmt.Printf("server %d vote for candidate %d in term %d \n", rf.me, args.CandidateId, rf.currentTerm)
			rf.chanGrantVote <- true
			reply.Term = args.Term
			reply.VoteGranted = true
			fmt.Printf("server %d becomes follower in term %d \n", rf.me, rf.currentTerm)
		}else {
			reply.Term = args.Term
			reply.VoteGranted = false
		}

	}else{
		if(rf.votedFor == -1 && args.LastLogIndex >= rf.getLastLogIndex() && args.LastLogTerm >= rf.getLastLogTerm()){
			rf.currentTerm = args.Term
			rf.votedFor = args.CandidateId
			fmt.Printf("server %d vote for candidate %d in term %d \n", rf.me, args.CandidateId, rf.currentTerm)
			rf.chanGrantVote <- true
			reply.Term = args.Term
			reply.VoteGranted = true
			fmt.Printf("server %d becomes follower in term %d \n", rf.me, rf.currentTerm)
		}else{
			reply.Term = args.Term
			reply.VoteGranted = false
		}
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	fmt.Printf("server %d send request to server %d %t \n", rf.me, server, ok)
	rf.mu.Lock()
	if(ok){
		if(rf.state == CANDIDATE && rf.currentTerm == reply.Term && reply.VoteGranted == true){
			rf.voteCount ++
			if(rf.voteCount > len(rf.peers)/2){
				fmt.Printf("sever %d becomes leader in term %d \n", rf.me, rf.currentTerm)
				rf.chanLeader <- true
			}
		}
		if(rf.state == CANDIDATE && rf.currentTerm < reply.Term){
			rf.currentTerm = reply.Term
			rf.chanHeartBeat <- true
			fmt.Printf("leader server %d becomes follower in term %d \n", rf.me, rf.currentTerm)
		}

	}else{
		if(rf.state == CANDIDATE){
			rf.peers[server].Call("Raft.RequestVote", args, reply)
			//fmt.Printf("server(Candidate) %d retry to send request vote to server %d in term %d \n", rf.me, server, rf.currentTerm)
		}
	}
	rf.mu.Unlock()
	return ok
}



func (rf *Raft) broadcastRequestVote(){

	fmt.Printf("server(Candidate) %d broadcast RequestVote in term %d \n", rf.me, rf.currentTerm)
	args := RequestVoteArgs{rf.currentTerm, rf.me, rf.getLastLogIndex(), rf.getLastLogTerm()}
	reply := &RequestVoteReply{}
	for index := range rf.peers{
		if(index != rf.me) {
			go rf.sendRequestVote(index, args, reply)
		}
	}

}




type AppendEntriesArgs struct {
	Term		int
	LeaderId	int
	PrevLogIndex	int
	PrevLogTerm	int
	Entries 	[]LogEntry
	LeaderCommit	int
}


type AppendEntriesReply struct {
	Term		int
	Success		bool
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	if(rf.currentTerm > args.Term){
		reply.Success = false
		reply.Term = rf.currentTerm
	}else{
		rf.votedFor = -1
		rf.currentTerm = args.Term
		reply.Success = true
		reply.Term = args.Term
		rf.chanHeartBeat <- true
	}

}



func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool{
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	if(ok && rf.state == LEADER && rf.currentTerm < reply.Term && !reply.Success){
		rf.currentTerm = reply.Term
		rf.state = FOLLOWER
		fmt.Printf("server(Leader) %d becomes follower in term %d \n", rf.me, rf.currentTerm)
	}else{
		if(rf.state == LEADER){
			rf.peers[server].Call("Raft.AppendEntries", args, reply)
			//fmt.Printf("server(leader) %d retry to send appendEntries rpc to server %d in term %d \n", rf.me, server, rf.currentTerm)
		}
	}
	rf.mu.Unlock()
	return ok
}



func (rf *Raft) boardcastAppendEntries(){
	//fmt.Printf("server(Leader): %d broadcast AppendEntries in term %d \n", me, rf.currentTerm)
	args := AppendEntriesArgs{rf.currentTerm, rf.me,  rf.getPrevLogIndex(), rf.getPrevLogTerm(), rf.log, rf.commitIndex}
	reply := &AppendEntriesReply{}
	for index := range rf.peers{
		if(index != rf.me){
			go rf.sendAppendEntries(index, args, reply)

		}
	}
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
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := rf.currentTerm
	isLeader := rf.state == LEADER
	if isLeader {
		index = rf.getLastLogIndex() + 1
		rf.log = append(rf.log, LogEntry{index, command, term})
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.state = FOLLOWER
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{Term:0})
	rf.currentTerm = 0
	rf.chanHeartBeat = make(chan bool)
	rf.chanCommit = make(chan bool, 100)
	rf.chanGrantVote = make(chan bool)
	rf.chanLeader = make(chan bool)
	rf.chanApplyMsg = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	go func(){
		for {
			switch  rf.state {
			case FOLLOWER :
				select {
				case <- rf.chanGrantVote:
				case <- rf.chanHeartBeat:
				case <-time.After(time.Duration(ElectionTimeoutConst()) * time.Millisecond):
					fmt.Printf("server(Follower) %d changes to candidate in term %d \n", rf.me, rf.currentTerm)
					rf.state = CANDIDATE
				}
			case LEADER :

				go rf.boardcastAppendEntries()
				time.Sleep(HBINTERVERL)
			case CANDIDATE :
				//rf.mu.Lock()
				rf.currentTerm ++
				rf.votedFor = rf.me
				rf.voteCount = 1
				//rf.mu.Unlock()
				fmt.Printf("server(Candidate) %d in term %d\n", me, rf.currentTerm)
				go rf.broadcastRequestVote()
				select {
				case <-time.After(time.Duration(ElectionTimeoutConst()) * time.Millisecond):
				case <-rf.chanHeartBeat:
					rf.state = FOLLOWER
				case <-rf.chanLeader:
					//rf.mu.Lock()
					rf.state = LEADER
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					for i := range rf.peers {
						rf.nextIndex[i] = rf.getLastLogIndex() + 1
						rf.matchIndex[i] = 0
					}
					//rf.mu.Unlock()
				}


			}
		}
	}()


	return rf
}



func ElectionTimeoutConst() int {
	res := rand.Intn(300) + 800
	return res

}