package raft

//添加状态
//2a的第二个reElection,是把主阻塞掉,什么也不干,看剩下的两台机器选主.但后来把另一个主页停了,有啥意义是?
//关于主"阻塞了",选出新主,过会新主死了,旧主活了的情况,要用原有协议而不另外添加机制的话,追加日志是可以追加的,但是在分发的时候会被拒绝
//现象陈述,2代主回来之后,3代主一直没给他发append?这个们自己似乎已经取消主的身份,但又选举成功了?
//2代主回来以后,lastRecvTime没有刷新,发起选主,这个是问题吗?---其实是可以刷新的,即遇到高任期号可以刷新下,但这里其实也不影响
//主超时要不要自杀?---在backup中就遇到了这个问题,被网络分区了要不要自杀
//
//
//
//我这边投出票后,没有重置超时时间
//加日志,选举世代,跳到新loop
//
//lastIndex---与主有日志不同,lastLog被覆盖了,得处理变化的清晰,怎么判断这种情况
//日志截断相关:startElection
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

import (
	"bytes"
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"time"

	"../labgob"
	"../labrpc"
)

// import "bytes"
// import "labgob"

//问题是除了leader以外,follower能知道哪些东西是已提交的吗?
//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
//快照相关的要放在这里?
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogItem struct {
	Index   int
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	ApplyCh   chan ApplyMsg       //后边这个是定语--按照要求,follower也要给回复
	Stop      bool

	State        int //follower1 leader2 candidate3
	Log          []LogItem
	CommitIndex  int
	LastLogIndex int
	LastLogTerm  int
	/*
		表示本机知道的已经提交的index,,对于follower没啥问题,跟着leader走就行了

		由于安全性原则,新主不会出现CommitIndex以上有实际已提交但我没提交的情况
		---实际应用时,如果这些日志不去提交,那tester端会持续等待.我这里的实现是加大锁,只能1个1个提交,倒是可以避免这个问题
		------不能一直等待啊,所以恐怕得加个当主后主动发空日志的情况
		------client连接时主发生切换会死很惨
	*/

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//跟选举有关的实时状态
	CurrentTerm      int   //当前所在任期
	LastRecvAppendMs int64 //上次收到append的时间

	//间隔时间组
	SleepInterval      int64 //用于定时选举的协程睡眠时间
	StartVoteInterval  int64 //启动投票超时
	SendAppendInterval int64 //主多久发一波append

	//作为candidate时的选举状态
	VoteFor       int   //这个只整一个跟crrent对应的就行了吧?
	VoteMe        []int //投我的有几个人,数组索引是选举轮次
	RPCTimes      []int //在某轮我发起的投票中,已收到了多少回复,用来标识此轮选举是否结束
	ElectionTimes int

	//leader专属
	NextIndex       []int //下一个要给follower发的日志号,初始化为下一个空日志号
	MatchIndex      []int //已复制的索引号---有啥用啊,暂时取消
	Ack             []int //leader发follower日志收到多少回复
	NextCommitIndex int   //标识我下一个要提交的位置,
	//---为了应对情形:刚当上主时有些日志没有提交,但安全性限制让我不能提交,
	//---需要在最新的日志位置先提交个,这个位置就是NextCommitIndex

	StartWork       bool //主专属,安全性限制,只有第一条提交了,才可以给follower推日志
	LeaderLoopTimes int
	LeaderLoopStop  [50]bool //50是估计最大选举轮次的值,用于停掉旧的leader loop

	//调试相关
	LoopRound       int
	LeaderLoopRound int
	AppendLoopRound [10]int
}

//这里似乎是测量标准
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.CurrentTerm
	if rf.State == 2 {
		isleader = true
	} else {
		isleader = false
	}

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
//给的例子是增量还是全量---因为内容不多,都不用增量,都是直接全量哈
//机器重启也是用的mak吗?
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	//fmt.Printf("peer:%d,in persist\n", rf.me)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.Log)
	e.Encode(rf.VoteFor)
	e.Encode(rf.CommitIndex)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)

	//fmt.Printf("peer:%d,persist over\n", rf.me)
}

//Persister传过来的只是一堆字节数组
//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var CurrentTerm int
	var Log []LogItem
	var VoteFor int
	var CommitIndex int
	//压码解码是按特定顺序的吗?---类型?
	if d.Decode(&CurrentTerm) != nil ||
		d.Decode(&Log) != nil ||
		d.Decode(&VoteFor) != nil ||
		d.Decode(&CommitIndex) != nil {
		//error...
		fmt.Printf("peer:%d restore failed\n", rf.me)
	} else {
		rf.CurrentTerm = CurrentTerm
		rf.Log = Log
		rf.VoteFor = VoteFor
		rf.CommitIndex = CommitIndex
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	CandidateId  int //机器标识
	Term         int //我要选的任期
	LastLogIndex int //最后收到?持久化的日志号
	LastLogTerm  int //最后日志任期
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

//心跳结构,leader使用的
type AppendEntries struct {
	Term int //仔细思考下,你想不到这个值,说明理解还不够透彻,为啥需要它?
	//关于leader和follower收到大任期号的情况,要变为follower,思考下---用这种绝对权威的方式,其实简化了问题
	LeaderId        int
	LeaderLoopRound int

	Entries      []LogItem
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int

	HistoryLogTerm []int
}

//心跳回包
type AppendEntriesReply struct {
	Term    int //当前任期号
	Success bool

	NextIndex int
}

//主第一次同follower交流,确定下一个发送位置
func (rf *Raft) getNextIndex(HistoryLogTerm []int) int {
	var upper int
	NextIndex := 0

	if len(rf.Log) < len(HistoryLogTerm) {
		upper = len(rf.Log) - 1
	} else {
		upper = len(HistoryLogTerm) - 1
	}

	//这么写循环,到时蛮符合论文中的规律的啦...
	for i := upper; i >= 0; i-- {
		if HistoryLogTerm[i] == rf.Log[i].Term {
			NextIndex = i + 1
			break
		}
	}

	return NextIndex
}

//followerer处理appendentries的handler
func (rf *Raft) AppendEntries(args *AppendEntries, reply *AppendEntriesReply) {
	fmt.Printf("peer:%d,recv append from %d,AppendLoopRound:%d\n", rf.me, args.LeaderId, args.LeaderLoopRound)

	if args.Term < rf.CurrentTerm {
		reply.Success = false
		reply.Term = rf.CurrentTerm
		fmt.Printf("peer:%d,refuse append from %d,AppendLoopRound:%d, his term: %d,my term:%d\n", rf.me, args.LeaderId, args.LeaderLoopRound, args.Term, rf.CurrentTerm)
	} else if args.Term == rf.CurrentTerm { //这地方同选举的地方有点混淆,这个判断需要看日志吗?
		reply.Success = true
		//在任期相等的情况下,也要成为follower---如果收到另一个leader的同等任期append,难道也要变么?漏洞太多---要分情况讨论
		if rf.State == 3 {
			rf.toFollower(args.Term)
		} else if rf.State == 1 {
			fmt.Printf("peer:%d,refresh lastrecvtime\n", rf.me)
			rf.LastRecvAppendMs = time.Now().UnixNano() / 1000000
		}

		//新主首次append,应该要确定NextIndex
		if len(args.HistoryLogTerm) > 0 {
			//要做截断,保证我的日志长度同主机保存的NextIndex一致
			reply.NextIndex = rf.getNextIndex(args.HistoryLogTerm)
			rf.Log = rf.Log[:reply.NextIndex]
			rf.persist()
			fmt.Printf("peer:%d, my nextindex is %d\n", rf.me, reply.NextIndex)
		} else {
			//这样也涵盖了没有日志空心跳的情形
			for i := 0; i < len(args.Entries); i++ {
				//可能是追加也可能是覆盖---都已经截断了就不会出现覆盖的情况了
				if args.Entries[i].Index >= len(rf.Log) {
					rf.Log = append(rf.Log, args.Entries[i])
				} else {
					rf.Log[args.Entries[i].Index] = args.Entries[i]
				}
			}

			if len(args.Entries) > 0 {
				rf.persist()
			}

			reply.NextIndex = len(rf.Log)

			fmt.Printf("peer:%d, recv log:\n", rf.me)
			for i := 0; i < len(args.Entries); i++ {
				fmt.Print(reflect.ValueOf(args.Entries[i].Command))
				fmt.Printf(":")
			}
			fmt.Printf("\n")

			rf.PrintLog()
		}

		//必须先有NextIndex才能确定CommitIndex
		//NextIndex必然大于CommitIndex
		oldCommitIndex := rf.CommitIndex

		if args.LeaderCommit > rf.CommitIndex {
			if len(rf.Log)-1 < args.LeaderCommit {
				//能确定此时我所有的日志都是该提交的?
				rf.CommitIndex = len(rf.Log) - 1
			} else {
				rf.CommitIndex = args.LeaderCommit
			}
			fmt.Printf("peer:%d, refresh commitindex from %d to %d\n", rf.me, oldCommitIndex, rf.CommitIndex)
		}

		//如果我之前当主已经提交过了,新回来了还要在提交一次吗?
		for i := oldCommitIndex + 1; i <= rf.CommitIndex; i++ {
			var msg ApplyMsg
			msg.Command = rf.Log[i].Command
			msg.CommandValid = true
			msg.CommandIndex = rf.Log[i].Index + 1

			rf.ApplyCh <- msg

			fmt.Printf("peer:%d, log commited,content:\n", rf.me)
			fmt.Println(rf.Log[i].Command)

			fmt.Printf("peer:%d, reply to tester\n", rf.me)
			fmt.Println(msg)
		}

	} else {
		rf.toFollower(args.Term)
		fmt.Printf("peer:%d, recv big term append from %d, %d, %d \n", rf.me, args.LeaderId, rf.CurrentTerm, args.Term)

		//跟上边有段重复的代码
		if len(args.HistoryLogTerm) > 0 {
			reply.NextIndex = rf.getNextIndex(args.HistoryLogTerm)
			fmt.Printf("peer:%d, meet big term, my nextindex is %d\n", rf.me, reply.NextIndex)
		}
	}

}

func (rf *Raft) LeaderCommitLog(startIndex int) {
	if startIndex-rf.CommitIndex > 1 {
		fmt.Printf("peer:%d, leader first time commit:\n", rf.me)
		for i := rf.CommitIndex + 1; i < startIndex; i++ {
			var msg ApplyMsg
			msg.Command = rf.Log[i].Command
			msg.CommandValid = true
			msg.CommandIndex = i + 1 //兼容哈

			rf.ApplyCh <- msg

			fmt.Printf("peer:%d, reply to tester,content:\n", rf.me)
			fmt.Println(msg)
		}
	}

	for i := startIndex; i < len(rf.Log); i++ {

		if rf.Ack[i] <= len(rf.peers)/2 {
			break
		}

		//提交,更新CommitIndex
		fmt.Printf("peer:%d, log commited,content:\n", rf.me)
		fmt.Println(rf.Log[i].Command)

		rf.CommitIndex = i
		rf.NextCommitIndex = i + 1
		fmt.Printf("peer:%d, refresh commitindex to %d\n", rf.me, rf.CommitIndex)

		var msg ApplyMsg
		msg.Command = rf.Log[i].Command
		msg.CommandValid = true
		msg.CommandIndex = i + 1 //兼容哈

		rf.ApplyCh <- msg

		fmt.Printf("peer:%d, reply to tester,content:\n", rf.me)
		fmt.Println(msg)
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntries, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//再次强调下,leaderLoopTimes是要传参数,不能用rf成员的
func (rf *Raft) AppendToFollower(server int, leaderLoopTimes int) {
	var argsEntity AppendEntries
	args := &argsEntity

	args.Term = rf.CurrentTerm
	args.LeaderId = rf.me
	//明显有的节点会卡住在某round
	args.LeaderLoopRound = rf.AppendLoopRound[server]
	args.LeaderCommit = rf.CommitIndex

	fmt.Printf("peer:%d,in AppendToFollower, append loop round:%d, send append to %d\n", rf.me, args.LeaderLoopRound, server)

	var reply AppendEntriesReply

	var ok bool
	startLogIndex := -1

	//首先要定位到从哪条开始推
	//要截断的话地方逻辑也要变
	if rf.NextIndex[server] == -1 {
		fmt.Printf("peer:%d, send all log to determine %d's nextindex\n", rf.me, server)
		//我这地方图方便就这么弄了,正常应该怎么处理?
		//---截断之后倒页可以比,毕竟CommitIndex之前的提交了,他那边没有就没有,一个rpc也能把应答给我了
		for j := 0; j < len(rf.Log); j++ {
			args.HistoryLogTerm = append(args.HistoryLogTerm, rf.Log[j].Term)
		}
	} else {
		startLogIndex = rf.NextIndex[server]
		fmt.Printf("peer:%d, to server:%d's nextindex: %d\n", rf.me, server, rf.NextIndex[server])
		//上边还是用,但这边意义就没有了
		//args.PrevLogIndex
		//args.PrevLogTerm

		if rf.StartWork {
			//这种区段用法,不需要检查数组是否有内容吗?---看日志是可以的,打出来区段长度为0
			args.Entries = rf.Log[startLogIndex:len(rf.Log)]
		} else if rf.CommitIndex >= startLogIndex {
			//在新主继任,还么startWork的情况下,也是可以传播并让follower提交一部分日志的
			args.Entries = rf.Log[startLogIndex : rf.CommitIndex+1]
		}
		fmt.Printf("peer:%d,to %d, send log index start from:%d, len:%d\n", rf.me, server, startLogIndex, len(args.Entries))

		for i := 0; i < len(args.Entries); i++ {
			fmt.Print(reflect.ValueOf(args.Entries[i].Command))
			fmt.Printf(":")
		}
		fmt.Printf("\n")
	}
	//所以说appen中要么是HistoryTerm探查NextCommitIndex,要么是日志,要么啥没有

	fmt.Printf("peer:%d, leader before send append to %d\n", rf.me, server)
	ok = rf.sendAppendEntries(server, args, &reply)
	fmt.Printf("peer:%d, leader append recv from server:%d\n", rf.me, server)
	fmt.Println(reply)

	//收包成功
	if ok {
		if reply.Success {
			if len(args.HistoryLogTerm) > 0 {
				rf.NextIndex[server] = reply.NextIndex
			} else if len(args.Entries) > 0 { //表明这次传输是有数据的,需要做回包统计,要更新CommitIndex
				//rf.NextIndex[server] += len(args.Entries)---这地方问题很大,因为存在截断的情况,不能是递增的
				rf.NextIndex[server] = reply.NextIndex
				fmt.Printf("peer:%d, success send log, refresh nextindex, %d, %d\n", rf.me, rf.NextIndex[server], len(args.Entries))
				for j := 0; j < len(args.Entries); j++ {
					fmt.Println(args.Entries[j])
					//理论上讲这个应该只有我发起的提交才会加这个接收次数和
					rf.Ack[args.Entries[j].Index]++
					fmt.Printf("peer:%d, incr log index:%d, ack:%d\n", rf.me, args.Entries[j].Index, rf.Ack[args.Entries[j].Index])
					//TestFailNoAgree2B里边测出来,这里会有后边的日志先提交的现象?
					//大哥这段代码一回合会跑几次啊???
					if rf.Ack[args.Entries[j].Index] > len(rf.peers)/2 &&
						args.Entries[j].Index == rf.NextCommitIndex {
						rf.LeaderCommitLog(args.Entries[j].Index)
					}

				}

			}
		} else {
			if reply.Term > rf.CurrentTerm {
				rf.toFollower(reply.Term)

				fmt.Printf("peer:%d, recv append reply from:%d, meet big term,trans to follower, start loop\n", rf.me, server)

			}
		}
		fmt.Printf("peer:%d,send append success, append loop:%d, send append to %d\n", rf.me, args.LeaderLoopRound, server)
	} else {
		//收包失败---这个库超时多大---老久了,直接堵死了
		fmt.Printf("peer:%d,send append failed, append loop round:%d, send append to %d,now:%d,retry\n", rf.me, args.LeaderLoopRound, server, time.Now().UnixNano()/1000000)
	}

}

//对单个节点定时发append消息
func (rf *Raft) AppendLoop(server int, leaderLoopTimes int) {
	fmt.Printf("peer:%d, start append routine to %d\n", rf.me, server)
	for {

		if rf.Stop == true {
			fmt.Printf("peer:%d, append loop stop\n", rf.me)
			break
		}

		if rf.LeaderLoopStop[leaderLoopTimes] {
			fmt.Printf("peer:%d, leader loop times:%d stop \n", rf.me, leaderLoopTimes)
			break
		}

		rf.AppendToFollower(server, leaderLoopTimes)

		//后边少写个0,造成频繁append每过TestCount
		time.Sleep(time.Duration(rf.SendAppendInterval * 1000000))
		rf.AppendLoopRound[server]++
	}
}

//你想想看,如果没有高任期号抹平机制,你leader状态我咋把你搞掉
func (rf *Raft) LeaderLoop() {

	fmt.Printf("peer:%d,start leader loop, times:%d\n", rf.me, rf.LeaderLoopTimes)

	for j := 0; j < len(rf.peers); j++ {
		if j != rf.me {
			go rf.AppendLoop(j, rf.LeaderLoopTimes)
		}

	}
}

//如果我更新或相同返回true,否则false
//如果选票过来了,对方赢返回false,那么就是说对方至少和我一样返回false
func (rf *Raft) CompareLog(args *RequestVoteArgs) bool {
	//我有日志,比较最后日志的任期和日志长度
	if len(rf.Log) > 0 {
		fmt.Printf("peer:%d,voteargs:", rf.me)
		fmt.Println(args)
		fmt.Printf("peer:%d,my log:%d %d\n", rf.me, rf.Log[len(rf.Log)-1].Term, len(rf.Log)-1)

		if args.LastLogTerm < rf.Log[len(rf.Log)-1].Term ||
			(args.LastLogTerm == rf.Log[len(rf.Log)-1].Term && args.LastLogIndex < len(rf.Log)-1) {
			return true
		} else {
			return false
		}
	} else { //这地方逻辑做了个大简化,我只要没有日志,对方肯定赢
		return false
	}
	/*
		else if args.LastLogTerm >= -1 { //对方有日志,我没有
			return false
		} else { //这是种什么情况
			return true
		}
	*/
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	fmt.Printf("peer:%d,recv voterequest from %d, his term:%d, my term:%d\n", rf.me, args.CandidateId, args.Term, rf.CurrentTerm)
	// Your code here (2A, 2B).
	biggerTerm := (args.Term > rf.CurrentTerm)
	if args.Term > rf.CurrentTerm {
		//如果我此时是leader,那么要停掉leader循环
		//2个问题,如果之前就是follower怎么办?要规范下toFollower的步骤
		rf.toFollower(args.Term)
	}

	//此处有问题啊...你变成follower之后任期会被拉高的...

	bLog := rf.CompareLog(args)
	fmt.Printf("peer:%d, bLog:%t, votefor:%d\n", rf.me, bLog, rf.VoteFor)

	//跟之前想的不同,同任期的情况下也选它
	//---同任期只能是挑战者自己增长的,不是隔离自增的话,就是同期大家竞争选主,但这种情况对方votefor是不会为空的,此种情形可忽略
	//---votefor可表示是否是candidate

	//你这个条件是怎么搞成这个样子的...
	//if args.Term >= rf.CurrentTerm && !bLog && rf.VoteFor == -1 {
	if biggerTerm && !bLog && rf.VoteFor == -1 {
		fmt.Printf("peer:%d,case 1, start loop\n", rf.me)
		reply.VoteGranted = true
		rf.VoteFor = args.CandidateId
		rf.persist()
		//怎么又回想起不同任期双主的问题了...
	} else {
		fmt.Printf("peer:%d,case 3\n", rf.me)
		reply.VoteGranted = false
	}

	reply.Term = rf.CurrentTerm
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
// 超时事件怎么设置的
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
//---听见没,可能阻塞半天,是否每个call都有返回,看下
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// 超时处理函数?---没有超时的意思?
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

//如果出现阻塞,这轮发起的选举事实上没有结束,routine死不了,RPC等值会被共用
//如果后来发起过新的选举,那么旧的选举自然而然就没有意义了
func (rf *Raft) GoLeaderElection(server int, electionTimes int, args *RequestVoteArgs) {
	fmt.Printf("peer:%d,toserver:%d, in GoLeaderElection, electionTimes:%d\n", rf.me, server, electionTimes)

	var reply RequestVoteReply

	//发一次就够了,这地方循环发送确实没啥必要,大不了别人选上嘛
	ok := rf.sendRequestVote(server, args, &reply)
	if ok {
		fmt.Printf("peer:%d,toserver:%d, sendRequestVote success\n", rf.me, server)
		fmt.Printf("peer:%d,reply vote:%t,term:%d\n", rf.me, reply.VoteGranted, reply.Term)

		//对方投我
		if reply.VoteGranted {
			rf.VoteMe[electionTimes]++

			//要查看,中途被搞成主或者变主之后又变成了follower,这个判断还是否有效?---就说说candidate状态也就持续开始的一小段时间,之后就是leader和follower的天下了
			if rf.VoteMe[electionTimes] >= len(rf.peers)/2+1 && rf.State == 3 && electionTimes == rf.ElectionTimes {
				rf.toLeader()
			}
		} else {
			//一种恶心情况,都已经被选为主了,却有收到一个拒绝回复,这种情况可能出现吗?---不要假设,要确信对方一定会出现
			if reply.Term > rf.CurrentTerm {
				rf.toFollower(reply.Term)

				fmt.Printf("peer:%d,recv vote reply, meet big term, trans to follower, start loop, %d, %d\n", rf.me, rf.CurrentTerm, reply.Term)
			}
		}
		//上述两种状态是不需要汇总的,但不能重复进入
		//另外还有一种平票拒绝的情况
	} else {
		//rpc失败
		fmt.Printf("peer:%d,toserver:%d,sendRequestVote fail\n", rf.me, server)
	}

	rf.RPCTimes[electionTimes]++
	if rf.RPCTimes[electionTimes] == len(rf.peers)-1 {
		//超时时间其实不必重置,初始化的时候随机下以后一直用就行了
		if rf.State == 3 {
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			LoopVoteInterval := (int64)(r.Intn(400))
			fmt.Printf("peer:%d,recv over,need election again,need sleep %dms\n", rf.me, LoopVoteInterval)
			time.Sleep(time.Duration(LoopVoteInterval * 1000000))
			fmt.Printf("peer:%d,wake up\n", rf.me)
			//此处需谨慎,也可能是旧的选举好不容易凑一块了,旧的选举消息自然作废了
			//---醒来之后状态其实会变的
			if electionTimes == rf.ElectionTimes && rf.State == 3 {
				rf.ElectionTimes++
				//fmt.Printf("peer:%d,inrc ElectionTimes, case 2, to: %d\n", rf.me, rf.ElectionTimes)
				rf.RPCTimes = append(rf.RPCTimes, 0)
				rf.VoteMe = append(rf.VoteMe, 1)
				//fmt.Printf("peer:%d,push rpctimes\n", rf.me)
				go rf.startLeaderElection(electionTimes + 1)
			}

		}
	}
}

//似乎搞混了,传入的这个值不是raft的成员,是个临时变量
func (rf *Raft) startLeaderElection(electionTimes int) {

	fmt.Printf("peer:%d,in startLeaderElection\n", rf.me)

	rf.mu.Lock()
	if rf.State == 1 {
		fmt.Printf("peer:%d,in startLeaderElection meet big term to become follower\n", rf.me)
		rf.mu.Unlock()
		return
	}

	//从原理上想,因为现在不是运行状态,加了下也没啥
	//因为当前任期值可能会被并发修改,包到锁范围中
	rf.CurrentTerm += 1
	rf.VoteFor = rf.me
	rf.persist()

	rf.mu.Unlock()

	var args RequestVoteArgs
	args.Term = rf.CurrentTerm
	args.CandidateId = rf.me

	//由于日志的截断问题,这两个值不要在这里维护
	//接收方如果没有日志,肯定就是我赢了
	/*
		if len(rf.Log) > 0 {
			args.LastLogIndex = len(rf.Log) - 1 //准备阶段后第一次遇到日志长度相关的
			args.LastLogTerm = rf.Log[len(rf.Log)-1].Term
		} else {
			args.LastLogIndex = -1
			args.LastLogTerm = -1
		}
	*/
	args.LastLogIndex = rf.LastLogIndex
	args.LastLogTerm = rf.LastLogTerm

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.GoLeaderElection(i, electionTimes, &args)
		}
	}

	//自然结束,汇总工作交给GoLeaderElection
}

//主循环,查看follower状态下是否收append超时---控制好,启动选举routine后就不能再跑了
//此处的情况是这样,我虽然在别人的选举中应答了别人,但是还没收到append的时候,我就开始恰好开始选举了,所以在回复选举票的时候,应该把LastRecv更新
func (rf *Raft) Loop() {

	//放外边是为了打日志从0开始,不在StartElection中是因为要做为StartElection的参数
	rf.ElectionTimes++
	for i := 0; ; i++ {
		if rf.Stop == true {
			fmt.Printf("peer:%d, main loop stop\n", rf.me)
			break
		}

		nowMs := time.Now().UnixNano() / 1000000
		diff := nowMs - rf.LastRecvAppendMs

		fmt.Printf("peer:%d,times:%d,loop:%d, now:%d, lastrecv:%d,diff:%d,interval:%d\n", rf.me, rf.ElectionTimes, rf.LoopRound, nowMs, rf.LastRecvAppendMs, diff, rf.StartVoteInterval)

		//state条件是后加的,你在选举状态时是不能再次启动选举的
		if diff > rf.StartVoteInterval && rf.State == 1 {
			fmt.Printf("peer:%d,startLeaderElection\n", rf.me)

			rf.mu.Lock()
			rf.State = 3 //先做个简单防御,这样routine切换过程中,我就可以发现中间被截胡了
			//加次数和推数组放在一起
			//rf.ElectionTimes++
			rf.RPCTimes = append(rf.RPCTimes, 0)
			rf.VoteMe = append(rf.VoteMe, 1)
			//fmt.Printf("peer:%d,push rpctimes\n", rf.me)
			rf.mu.Unlock()

			//fmt.Printf("peer:%d,inrc ElectionTimes, case 1, to: %d\n", rf.me, rf.ElectionTimes)

			go rf.startLeaderElection(rf.ElectionTimes)
			break
			//这种模式的问题在于,如果你发起的选举卡住了,就不会再发起选举
			//后来考虑过要不要继续维持这个loop,先算了,先不要增加复杂性,上轮选举未结束的情况下这轮loop很尴尬
		} else {
			time.Sleep(time.Duration(rf.SleepInterval * 1000000))
		}
		rf.LoopRound++
	}
}

func (rf *Raft) PrintLog() {
	fmt.Printf("peer:%d,all log:", rf.me)

	for i := 0; i < len(rf.Log); i++ {
		fmt.Print(reflect.ValueOf(rf.Log[i]))
		fmt.Printf(":")
	}
	fmt.Printf("\n")
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
//安全性限制,我最新提交一个新值才能开始传旧值,在实现时,具体放宽到收到新请求就可以开始了
//不等提交,立刻返回?
//start加锁范围要宽些,毕竟可以在外部多线程调用,TestUnreliableAgree2C测出来的
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	//fmt.Printf("peer:%d,in start\n", rf.me)

	index := -1
	term := -1
	isLeader := false

	//rf.mu.Lock()
	//被第一段最后一句话吓的
	if rf == nil {
		return index, term, isLeader
	}

	//也可能处于sb候选人状态哦
	if rf.State != 2 {
		//fmt.Printf("peer:%d,not leader\n", rf.me)
		return index, term, isLeader
	}

	fmt.Printf("peer:%d,in start log content:", rf.me)
	pstr := reflect.ValueOf(command)
	fmt.Println(pstr)

	// Your code here (2B).
	//塞到内存结构里就行了

	rf.mu.Lock()
	var item LogItem
	item.Command = command
	//item.Index = len(rf.Log)
	//这个值也只有在这种情况下才会增长
	rf.LastLogIndex = rf.LastLogIndex + 1
	item.Index = rf.LastLogIndex
	item.Term = rf.CurrentTerm

	rf.Log = append(rf.Log, item) //append定期检查的时候查看是否需要发送部分日志---是不是得看看这个操作有没有用
	rf.persist()
	//index = len(rf.Log) //暂时兼容下
	index = rf.LastLogIndex + 1
	rf.mu.Unlock()

	//rf.Ack[len(rf.Log)-1]++ //之前没写,自己应该先投个票,bug
	rf.Ack[rf.LastLogIndex]++

	rf.PrintLog()

	//不用等结果,有请求就算正式开始了吗?
	//---你这条不设置成真的,发不了日志,那成了死锁了,21军规
	rf.StartWork = true

	//index = len(rf.Log) - 1
	//index = len(rf.Log) //暂时兼容下
	term = rf.CurrentTerm
	isLeader = true

	fmt.Printf("peer:%d,return the test start value:%d\n", rf.me, index)

	return index, term, isLeader //tester并发调start的时候,出现问题,返回的并不是23456,因为多routine同步操作Log
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	fmt.Printf("peer:%d killed\n", rf.me)

	//follower状态下main loop停掉,leader状态下append loop停掉,candidate状态下直接杀了吧
	rf.Stop = true

	rf.PrintLog()

	time.Sleep(time.Duration(10 * 1000000)) //少睡会
	nowMs := time.Now().UnixNano() / 1000000
	fmt.Printf("peer:%d kill sleep over,now:%d\n", rf.me, nowMs)

}

//看看咋用channel恢复个ApplyMsg--a实验是不是先用不到
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
	fmt.Printf("peer:%d,make\n", me)

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	// initialize from state persisted before a crash
	rf.me = me
	rf.ApplyCh = applyCh
	rf.Stop = false

	// Your initialization code here (2A, 2B, 2C).

	//初始状态为follower,可别一上来就candidate发起选举,等会
	rf.State = 1
	//Log
	rf.CommitIndex = -1

	rf.CurrentTerm = 0
	rf.LastRecvAppendMs = time.Now().UnixNano() / 1000000

	//按照没秒10次append计算---发现其实这个没啥意义,太频繁了会超过测试程序的限制
	rf.SleepInterval = 200
	//rf.StartVoteInterval = 300 + rf.getRand(150)
	rf.StartVoteInterval = 500 + rf.getRand(500)

	//文档里说了1s中不能发送超过10次....把代码中与时间有关的地方都做协调性的修改
	//rf.SendAppendInterval = 200
	rf.SendAppendInterval = 200

	rf.VoteFor = -1
	//VoteMe
	//RPCTimes,这俩是vector,得每次选举的时候插入值
	//作为candidate时的选举状态,为了处理bug加的
	rf.ElectionTimes = -1

	//如果不是批量发的话,这俩会有什么不同吗
	for i := 0; i < len(rf.peers); i++ {
		rf.MatchIndex = append(rf.MatchIndex, -1)
		rf.NextIndex = append(rf.NextIndex, -1)
	}

	for i := 0; i < 2000; i++ {
		rf.Ack = append(rf.Ack, 0)
	}
	rf.NextCommitIndex = 0

	//StartWork
	rf.LeaderLoopTimes = -1
	for i := 0; i < len(rf.LeaderLoopStop); i++ {
		rf.LeaderLoopStop[i] = false
	}

	//一个问题,我还没超时,收到你的投你的请求,该怎么处理?--应该忽略--有种情况是我知道主已死,但我还没到发请求的超时时间--事实证明我该变成follower
	//认定主已死算要超时,循环发起投票也是超时,这里区别?--我怎么忘了为啥后者要比前者大的原因了?--论文列没区别,你自己搞出的区别
	//没有统一的整齐划一的大家一起重新开始超时
	//发append没写--在选举期间收到append也没写--投票和append的混杂,处理好
	//follower回不了包也要启动选举?在这个环境里咋操作啊...这里没法搞回包的是否完成判断啊...

	//调试相关
	rf.LoopRound = 0
	rf.LeaderLoopRound = 0
	for i := 0; i < len(peers); i++ {
		rf.AppendLoopRound[i] = 0
	}

	fmt.Printf("peer:%d,lastRecv:%d\n", rf.me, rf.LastRecvAppendMs)

	//放前边岂不是有些值走了正常的初始化,没有从persist中取
	rf.readPersist(persister.ReadRaftState())

	//需要等一下这个协程吗
	go rf.Loop()

	return rf
}

func (rf *Raft) getRand(up int) int64 {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	ret := (int64)(r.Intn(up))
	return ret
}

func (rf *Raft) LeaderCommitEmptyLog() {

	rf.mu.Lock()
	var item LogItem
	item.Command = 10086
	item.Index = len(rf.Log)
	item.Term = rf.CurrentTerm

	rf.Log = append(rf.Log, item) //append定期检查的时候查看是否需要发送部分日志---是不是得看看这个操作有没有用
	rf.persist()
	rf.Ack[len(rf.Log)-1]++ //之前没写,自己应该先投个票,bug
	rf.PrintLog()
	rf.StartWork = true
	rf.mu.Unlock()
}

func (rf *Raft) toLeader() {
	//用状态作为判断标准,进行卡位
	rf.State = 2
	rf.StartVoteInterval = 2000000000000000

	//要考虑到多次被选成主的情况
	for i := 0; i < len(rf.peers); i++ {
		rf.MatchIndex[i] = -1
		rf.NextIndex[i] = -1
	}

	//rf.NextCommitIndex = len(rf.Log)
	rf.NextCommitIndex = rf.LastLogIndex + 1

	for i := 0; i < 2000; i++ {
		rf.Ack[i] = 0
	}

	rf.StartWork = false
	fmt.Printf("peer:%d,become leader\n", rf.me)

	//应该停掉不假,但这个事情不应该拖延到现在才做啊?
	rf.LeaderLoopTimes++
	/*
		if rf.LeaderLoopTimes > 0 {
			rf.LeaderLoopStop[rf.LeaderLoopTimes-1] = true
		}
	*/

	rf.VoteFor = -1

	rf.persist()

	go rf.LeaderLoop()

	//rf.LeaderCommitEmptyLog()
}

func (rf *Raft) toFollower(term int) {

	rf.mu.Lock()
	//原来是放在if里边的,如果原来状态就是1,这个也要刷新一下,不然的话马上发起选举,用刚刚得到的高term去扰乱秩序
	rf.LastRecvAppendMs = time.Now().UnixNano() / 1000000
	if rf.State == 2 || rf.State == 3 {

		//leader只会向这一个方向转移
		if rf.State == 2 {
			rf.LeaderLoopStop[rf.LeaderLoopTimes] = true
		}

		fmt.Printf("peer:%d,case 1, start loop\n", rf.me)
		//旧loop删掉了吗
		go rf.Loop()
	}

	rf.State = 1
	rf.CurrentTerm = term
	rf.VoteFor = -1
	rf.persist()
	rf.StartVoteInterval = 300 + rf.getRand(500)
	rf.mu.Unlock()
}

//这边不要由上边设置界限,直接我自己吧CommitIndex之前的都扔了就完了
func (rf *Raft) DiscardLog(index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//你这么截断,日志号没法维护了...---积累历史清楚索引长度
	//rf.Log = rf.Log[:index+1]

	if index > rf.LastLogIndex {
		rf.Log.clear()
	}

	if len(rf.Log) > 0 {
		if index < rf.Log[0].Index {
			return
		}

		if index > rf.Log[len(rf.Log)-1].Term {
			rf.Log.clear()
			return
		}

		var ClearIndex int

		for i := 0; i < len(rf.Log); i++ {
			if index == rf.Log[i].Index {
				ClearIndex = i
			}
		}

		rf.Log = rf.Log[ClearIndex:+1]
	} else { //没有日志就不用截断了
		return
	}

	rf.persist()
}

//要是有截断的话这个就完全没法用了...
func (rf *Raft) SearchLog(command interface{}) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := len(rf.Log) - 1; i >= 0; i-- {
		if command == rf.Log[i].Command {
			return true
		}
	}

	return false
}
