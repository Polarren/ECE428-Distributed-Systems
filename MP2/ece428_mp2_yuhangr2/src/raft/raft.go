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

import "time"
import "math/rand"
import "sync"
import "sync/atomic"
import "../labrpc"
//import "fmt"
//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// You may also need to add other state, as per your implementation.
	state       int               // 0 for Follower; 1 for Cnadidate; 2 for Leader
	currentTerm int               //latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int               //candidateId that received vote in current term (or null if none)
	logs         []LogEntry        //log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	electionTimer  *time.Timer 
	heartbeatTimer *time.Timer

	applyCh     chan ApplyMsg
	//Volatile state on all servers:
    commitIndex int //index of highest log entry known to be committed (initialized to 0, increases monotonically)
    lastApplied int //index of highest log entry applied to state machine (initialized to 0, increases monotonically)
    //Volatile state on leaders:(Reinitialized after election)
    nextIndex  []int //for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
    matchIndex []int //for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
}

type LogEntry struct {
	Term    int
    Command interface{}
}

const (
    Follower           = 0
    Candidate          = 1
    Leader             = 2
    
)

const ( //unit in ms
	HeartbeatInterval  = 110 * time.Millisecond
	MinElectionTimeout = 200
	MaxElectionTimeout = 400
)
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state==Leader // 0 for Follower; 1 for Cnadidate; 2 for Leader
	rf.mu.Unlock()
	return term, isleader
}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term            int     //candidate’s term
	CandidateId     int     //candidate requesting vote
	LastLogIndex    int     //index of candidate’s last log entry (§5.4)
	LastLogTerm     int     //term of candidate’s last log entry (§5.4)
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term            int     //currentTerm, for candidate to update itself
	VoteGranted     bool    //true means candidate received vote
}
//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// Read the fields in "args", 
	// and accordingly assign the values for fields in "reply".
	//fmt.Println("Process ",args.CandidateId, "is requesting vote from Process ",rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		// rf.votedFor = -1
		rf.convertTo(Follower)
	}
	
	reply.Term = args.Term

	//! denies its vote if its own log is more up-to-date than that of the candidate
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.VoteGranted = false
	} else if rf.logs[len(rf.logs)-1].Term > args.LastLogTerm {	//! term
		reply.VoteGranted = false
	} else if rf.logs[len(rf.logs)-1].Term == args.LastLogTerm && len(rf.logs)-1 > args.LastLogIndex {
		reply.VoteGranted = false
	} else {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		resetTimer(rf.electionTimer,randTimeout(MinElectionTimeout, MaxElectionTimeout))
	}

}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int        
	PrevLogTerm  int        
	Entries   []LogEntry  
	LeaderCommit int        
}

type AppendEntriesReply struct {
	Term    int  
	Success bool 

}

// AppendEntries function
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    
    //reply.Success = true

    // 1. Reply false if term < currentTerm (§5.1)
    if args.Term < rf.currentTerm {
        reply.Success = false
        reply.Term = rf.currentTerm
        return
    }
    //If RPC request or response contains term T > currentTerm:set currentTerm = T, convert to follower (§5.1)
    if args.Term > rf.currentTerm {
        rf.currentTerm = args.Term
        rf.convertTo(Follower)
    }

    // reset election timer even log does not match
    // args.LeaderId is the current term's Leader
    rf.electionTimer.Reset(randTimeout(MinElectionTimeout,MaxElectionTimeout))

    // 2. Reply false if log doesn’t contain an entry at prevLogIndex
    // whose term matches prevLogTerm (§5.3)
    lastLogIndex := len(rf.logs) - 1
    if lastLogIndex < args.PrevLogIndex {
        reply.Success = false
        reply.Term = rf.currentTerm
        return
    }

    if rf.logs[(args.PrevLogIndex)].Term != args.PrevLogTerm {
        reply.Success = false
        reply.Term = rf.currentTerm
        return
    }

	// 3. If an existing entry conflicts with a new one (same index
    // but different terms), delete the existing entry and all that
    // follow it (§5.3)
	
    // 4. Append any new entries not already in the log
    // compare from rf.logs[args.PrevLogIndex + 1]
    unmatch_idx := -1
    for idx := range args.Entries {
        if len(rf.logs) < (args.PrevLogIndex+2+idx) ||
            rf.logs[(args.PrevLogIndex+1+idx)].Term != args.Entries[idx].Term {
            // unmatch log found
            unmatch_idx = idx
            break
        }
    }

    if unmatch_idx != -1 {
        // there are unmatch entries
        // truncate unmatch Follower entries, and apply Leader entries
        rf.logs = rf.logs[:(args.PrevLogIndex + 1 + unmatch_idx)]
        rf.logs = append(rf.logs, args.Entries[unmatch_idx:]...)
    }

    //5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
    if args.LeaderCommit > rf.commitIndex {
        rf.setCommitIndex(min(args.LeaderCommit, len(rf.logs)-1))
    }

    reply.Success = true
}

// should be called with lock
func (rf *Raft) heartbeats() {
    for i := range rf.peers {
        if i != rf.me {
            go rf.heartbeat(i)
        }
    }
}

func (rf *Raft) heartbeat(server int) {
    rf.mu.Lock()
    if rf.state != Leader {
        rf.mu.Unlock()
        return
    }

    prevLogIndex := rf.nextIndex[server] - 1
	
    // use deep copy to avoid race condition
    // when override log in AppendEntries()
    entries := make([]LogEntry, len(rf.logs[(prevLogIndex+1):]))
    copy(entries, rf.logs[(prevLogIndex+1):])
	
	//fmt.Println("Sending heartbeat,prevLogIndex=",prevLogIndex)

    args := AppendEntriesArgs{
        Term:         rf.currentTerm,
        LeaderId:     rf.me,
        PrevLogIndex: prevLogIndex,
        PrevLogTerm:  rf.logs[prevLogIndex].Term,
        Entries:      entries,
        LeaderCommit: rf.commitIndex,
    }
    rf.mu.Unlock()

    var reply AppendEntriesReply
    if rf.sendAppendEntries(server, &args, &reply) {
        rf.mu.Lock()
        if rf.state != Leader {
            rf.mu.Unlock()
            return
        }
        // If last log index ≥ nextIndex for a follower: send
        // AppendEntries RPC with log entries starting at nextIndex
        // • If successful: update nextIndex and matchIndex for
        // follower (§5.3)
        // • If AppendEntries fails because of log inconsistency:
        // decrement nextIndex and retry (§5.3)
        if reply.Success {
            // successfully replicated args.Entries
            rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
            rf.nextIndex[server] = rf.matchIndex[server] + 1

            // If there exists an N such that N > commitIndex, a majority
            // of matchIndex[i] ≥ N, and log[N].term == currentTerm:
            // set commitIndex = N (§5.3, §5.4).
            for N := (len(rf.logs) - 1); N > rf.commitIndex; N-- {
                count := 0
                for _, matchIndex := range rf.matchIndex {
                    if matchIndex >= N {
                        count += 1
                    }
                }

                if count > len(rf.peers)/2 {
                    // most of nodes agreed on rf.logs[i]
                    rf.setCommitIndex(N)
                    break
                }
            }

        } else {
            if reply.Term > rf.currentTerm {
                rf.currentTerm = reply.Term
                rf.convertTo(Follower)
            } else {
                rf.nextIndex[server] = args.PrevLogIndex - 1
            }
        }
        rf.mu.Unlock()
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
    defer rf.mu.Unlock()
    term = rf.currentTerm
    isLeader = rf.state == Leader
    if isLeader {
        rf.logs = append(rf.logs, LogEntry{Command: command, Term: term})
        index = len(rf.logs) - 1
        rf.matchIndex[rf.me] = index
        rf.nextIndex[rf.me] = index + 1
    }

    return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// Chage the state of a raft server
// should be called with a lock
func (rf *Raft) convertTo(s int) {
	if s == rf.state {
		return
	}
	rf.state = s
	switch s {
	case Follower:
		rf.heartbeatTimer.Stop()
		rf.electionTimer.Reset(randTimeout(MinElectionTimeout, MaxElectionTimeout))
		rf.votedFor = -1

	case Candidate:
		rf.startElection()

	case Leader:
		//fmt.Println("Process",rf.me,"wins the election")
		rf.electionTimer.Stop()
		rf.heartbeats()
		rf.heartbeatTimer.Reset(HeartbeatInterval)
	}
}

// Normal operation of a raft
func(rf *Raft) maintain() {
	for {
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			// we know the timer has stopped now
			// no need to call Stop()
			rf.electionTimer.Reset(randTimeout(MinElectionTimeout, MaxElectionTimeout))
			if rf.state == Follower {
				// Raft::startElection() is called in conversion to Candidate
				rf.convertTo(Candidate)
			} else {
				rf.startElection()
			}
			rf.mu.Unlock()

		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == Leader {
				rf.heartbeats()
				// we know the timer has stopped now
				// no need to call Stop()
				rf.heartbeatTimer.Reset(HeartbeatInterval)
			}
			rf.mu.Unlock()
		}
	}
}

// Start election
// should be called with lock
func (rf *Raft) startElection() {

	rf.currentTerm += 1

	lastLogIndex := len(rf.logs) - 1
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  rf.logs[lastLogIndex].Term,
	}

	var voteCount int32

	for i := range rf.peers {
		if i == rf.me {
			rf.votedFor = rf.me
			atomic.AddInt32(&voteCount, 1)
			continue
		}
		go func(server int) {
			var reply RequestVoteReply
			if rf.sendRequestVote(server, &args, &reply) {
				rf.mu.Lock()
				if reply.VoteGranted && rf.state == Candidate {
					atomic.AddInt32(&voteCount, 1)
					if atomic.LoadInt32(&voteCount) > int32(len(rf.peers)/2) {
						rf.convertTo(Leader)
					}
				} else {
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.convertTo(Follower)
					}
				}
				rf.mu.Unlock()
			}
		}(i)
	}
}

// return a random integer as the number of milliseconds of timeout value
func randTimeout(min, max int) time.Duration {
	//rand.Seed(time.Now().UnixNano())
	num := rand.Intn(max - min) + min
	return time.Millisecond* time.Duration(num)
}

// reset a timer
func resetTimer(timer *time.Timer, d time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C: //try to drain from the channel
		default:
		}
	}
	timer.Reset(d)
}

func (rf *Raft) setCommitIndex(commitIndex int) {
    rf.commitIndex = commitIndex
    // apply all entries between lastApplied and committed
    // should be called after commitIndex updated
    if rf.commitIndex > rf.lastApplied {
        entriesToApply := append([]LogEntry{}, rf.logs[(rf.lastApplied+1):(rf.commitIndex+1)]...)

        go func(startIdx int, entries []LogEntry) {
            for idx, entry := range entries {
                var msg ApplyMsg
                msg.CommandValid = true
                msg.Command = entry.Command
                msg.CommandIndex = startIdx + idx
                rf.applyCh <- msg
                // do not forget to update lastApplied index
                // this is another goroutine, so protect it with lock
                rf.mu.Lock()
                if rf.lastApplied < msg.CommandIndex {
                    rf.lastApplied = msg.CommandIndex
                }
                rf.mu.Unlock()
            }
        }(rf.lastApplied+1, entriesToApply)
    }
}
func min(x, y int) int {
    if x < y {
        return x
    } else {
        return y
    }
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.me = me

	// Your initialization code here (2A, 2B).
	rf.state =Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.heartbeatTimer = time.NewTimer(HeartbeatInterval)
	randtime:=randTimeout(MinElectionTimeout, MaxElectionTimeout)
	rf.electionTimer = time.NewTimer(randtime)
	//fmt.Println("Random election timeout value for Process",rf.me,":",randtime)
	rf.applyCh = applyCh
	rf.logs = make([]LogEntry, 1)

    rf.nextIndex = make([]int, len(rf.peers))
    rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		// initialized to leader last log index + 1
		rf.nextIndex[i] = len(rf.logs)
	}
	go rf.maintain()
	return rf
}
