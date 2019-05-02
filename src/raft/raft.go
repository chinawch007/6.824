package raft

//A的目的是实现心跳和选主
//添加状态
//2a的第二个reElection,是把主阻塞掉,什么也不干,看剩下的两台机器选主.但后来把另一个主页停了,有啥意义是?
//
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

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	CurrentTerm int //当前所在任期
	VoteFor     int //这个只整一个跟crrent对应的就行了吧?
	//先把最后日志的状态放在这里吧
	//LastLogIndex int
	//LastLogTerm  int

	State int //follower1 leader2 candidate3

	SleepInterval     int64
	StartVoteInterval int64 //启动投票超时
	LoopVoteInterval  int64 //作为candidate多久发起一次投票

	SendAppendInterval int64 //主多久发一波append

	//作为candidate时的选举状态
	VoteMe   int //投我的有几个人
	RPCTimes int //已进行多少次RPC,无论成功失败

	//append相关
	LastRecvAppendMs int64 //上次收到append的时间

	ApplyCh chan ApplyMsg //后边这个是定语--按照要求,follower也要给回复
	Log     []LogItem
	//那么这个字段leader和follower之间还有些不同
	//这个字段的维护还是个问题啊,新主这个字段怎么搞,还牵涉到安全性限制
	//关于这个字段的意义,如果按照主就是标准的话,这个字段有什么用?
	//关系到给tester的回复,我需要知道具体到哪里是提交的---安全性限制,我得先提交一个,之前的连锁反应,就都算提交了,不然的话,提交进度保持不变
	//不错,新主第一条日志,强制更新CommitIndex
	CommitIndex int //这地方还是有些不明白,follower怎么才能知道一个日志提没提交?---在append中主通知的,这么说来是异步更新的

	//leader专属
	NextIndex  []int //下一个要给follower发的日志号,初始化为下一个空日志号
	MatchIndex []int //已复制的索引号---有啥用啊,暂时取消
	Ack        []int

	StartWork bool //主专属,安全性限制,只有第一条提交了,才可以给follower推日志

	//调试相关
	LoopRound       int
	LeaderLoopRound int
	Stop            bool
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

//投票投了谁也要持久化,复杂
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
	fmt.Printf("peer:%d,in persist\n", rf.me)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.Log)
	e.Encode(rf.VoteFor)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)

	fmt.Printf("peer:%d,persist over\n", rf.me)
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
	//压码解码是按特定顺序的吗?---类型?
	if d.Decode(&CurrentTerm) != nil ||
		d.Decode(&Log) != nil ||
		d.Decode(&VoteFor) != nil {
		//error...
	} else {
		rf.CurrentTerm = CurrentTerm
		rf.Log = Log
		rf.VoteFor = VoteFor
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
//在写2b的时候需不需要以固定频率发送,看看可不可以改下,减少发送的频率
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

func (rf *Raft) getNextIndex(HistoryLogTerm []int) int {
	var upper int
	NextIndex := 0

	if len(rf.Log) < len(HistoryLogTerm) {
		upper = len(rf.Log) - 1
	} else {
		upper = len(HistoryLogTerm) - 1
	}

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
	fmt.Printf("peer:%d,recv append,AppendLoopRound:%d\n", rf.me, args.LeaderLoopRound)

	if args.Term < rf.CurrentTerm {
		reply.Success = false
		reply.Term = rf.CurrentTerm
	} else if args.Term == rf.CurrentTerm {
		reply.Success = true
		//在任期相等的情况下,也要成为follower---如果收到另一个leader的同等任期append,难道也要变么?漏洞太多---要分情况讨论
		if rf.State == 3 {
			fmt.Printf("peer:%d,from candidate to follower\n", rf.me)
			rf.State = 1
			//go rf.Loop()---大哥,我如果是candidate的话原来就有loop啊...
		} else if rf.State == 1 {
			fmt.Printf("peer:%d,refresh lastrecvtime\n", rf.me)
			rf.LastRecvAppendMs = time.Now().UnixNano() / 1000000
		}

		//新主首次append,应该要确定NextIndex
		if len(args.HistoryLogTerm) > 0 {
			reply.NextIndex = rf.getNextIndex(args.HistoryLogTerm)
			fmt.Printf("peer:%d, my nextindex is %d\n", rf.me, reply.NextIndex)

			//要做截断,保证我的日志长度同主机保存的NextIndex一致
			rf.Log = rf.Log[:reply.NextIndex]
		} else {

			for i := 0; i < len(args.Entries); i++ {
				//可能是追加也可能是覆盖
				if args.Entries[i].Index >= len(rf.Log) {
					rf.Log = append(rf.Log, args.Entries[i])
				} else {
					rf.Log[args.Entries[i].Index] = args.Entries[i]
				}
			}

			fmt.Printf("peer:%d, recv log:\n", rf.me)
			for i := 0; i < len(args.Entries); i++ {
				fmt.Print(reflect.ValueOf(args.Entries[i].Command))
				fmt.Printf(":")
			}
			fmt.Printf("\n")

			rf.persist()

			rf.PrintLog()
			/*
				//确实,只有收到新日志的时候,CommitIndex才会动,不然的话自然保持不变
				oldCommitIndex := rf.CommitIndex

				//这地方逻辑是不对的,没有数据,CommitIndex也会异步更新
				if len(args.Entries) > 0 {
					if len(rf.Log)-1 < args.LeaderCommit {
						rf.CommitIndex = len(rf.Log) - 1
					} else {
						rf.CommitIndex = args.LeaderCommit
					}
				}

				for i := oldCommitIndex + 1; i <= rf.CommitIndex; i++ {
					var msg ApplyMsg
					msg.Command = rf.Log[i].Command
					msg.CommandValid = true
					msg.CommandIndex = rf.Log[i].Index + 1

					rf.ApplyCh <- msg
				}
			*/
		}

		//处理下CommitIndex的问题
		//必须先有NextIndex才能确定CommitIndex
		//NextIndex必然大于CommitIndex
		oldCommitIndex := rf.CommitIndex

		if args.LeaderCommit > rf.CommitIndex {
			if len(rf.Log)-1 < args.LeaderCommit {
				rf.CommitIndex = len(rf.Log) - 1
			} else {
				rf.CommitIndex = args.LeaderCommit
			}
		}

		for i := oldCommitIndex + 1; i <= rf.CommitIndex; i++ {
			var msg ApplyMsg
			msg.Command = rf.Log[i].Command
			msg.CommandValid = true
			msg.CommandIndex = rf.Log[i].Index + 1

			rf.ApplyCh <- msg
		}

		//如果此时有选举,应该使其停止,跟之前的投票期间截胡是一个原理,在发起投票和投票汇总函数中做防御性处理
	} else { //事实上这条大任期号强制性原则创造了很多你之前想像不到的情形---保持状态不动,还是follower
		rf.CurrentTerm = args.Term
		rf.State = 1 //把这条s加上吧,感觉总会有我想不到的角落情形---哦,担心比如不是follower的状态收到这条信息

		rf.persist()

		//这地方是我加的,被截胡后,更新最后接收时间
		rf.LastRecvAppendMs = time.Now().UnixNano() / 1000000

		if len(args.HistoryLogTerm) > 0 {
			reply.NextIndex = rf.getNextIndex(args.HistoryLogTerm)
			fmt.Printf("peer:%d, my nextindex is %d\n", rf.me, reply.NextIndex)
		} else {
			for i := 0; i < len(args.Entries); i++ {
				rf.Log[args.Entries[i].Index] = args.Entries[i]
			}

			fmt.Printf("peer:%d, recv log:\n", rf.me)
			for i := 0; i < len(args.Entries); i++ {
				fmt.Print(reflect.ValueOf(args.Entries[i].Command))
				fmt.Printf(":")
			}
			fmt.Printf("\n")

			rf.persist()

			rf.PrintLog()
		}

		go rf.Loop()
		//如果此时有选举,应该使其停止,跟之前的投票期间截胡是一个原理,在发起投票和投票汇总函数中做防御性处理
	}

	//---顺便更新commitIndex

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntries, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//关于一次推几条日志的问题,
func (rf *Raft) LeaderAppend(server int) {
	var argsEntity AppendEntries
	args := &argsEntity

	args.Term = rf.CurrentTerm
	args.LeaderId = rf.me
	args.LeaderLoopRound = rf.AppendLoopRound[server]

	args.LeaderCommit = rf.CommitIndex

	//fmt.Printf("peer:%d,in GoLeaderAppend, append loop:%d, send append to %d\n", rf.me, args.LeaderLoopRound, server)
	var reply AppendEntriesReply

	var ok bool
	startLogIndex := -1

	//首先要定位到从哪条开始推
	if rf.NextIndex[server] == -1 { //不单独搞个rpc了,复用append的,简单点
		fmt.Printf("peer:%d, start to determine %d's nextindex\n", rf.me, server)
		for j := 0; j < len(rf.Log); j++ {
			args.HistoryLogTerm = append(args.HistoryLogTerm, rf.Log[j].Term) //这么 操作也不好不好使
		}
	} else {
		startLogIndex = rf.NextIndex[server]
		//按照我的思路,这两个值似乎没必要了,我的leader第一次跟follower沟通,就是要确定发送位置,不需要你这个sb中介了
		//上边还是用,但这边意义就没有了
		//args.PrevLogIndex
		//args.PrevLogTerm

		if rf.StartWork {
			//args.Entries = rf.Log[startLogIndex : len(rf.Log)-1] //能保证正确吗?前边值比后边值大的情况下
			args.Entries = rf.Log[startLogIndex:len(rf.Log)] //之前用法错了
			fmt.Printf("peer:%d, send args log len:%d\n", rf.me, len(args.Entries))
		}
		fmt.Printf("peer:%d, start send entry, from %d\n", rf.me, startLogIndex)

		for i := 0; i < len(args.Entries); i++ {
			fmt.Print(reflect.ValueOf(args.Entries[i].Command))
			fmt.Printf(":")
		}
		fmt.Printf("\n")
	}

	for i := 0; ; i++ {
		ok = rf.sendAppendEntries(server, args, &reply)
		//收包成功
		if ok {
			if reply.Success {
				//收包成功了需要做啥不
				//分情况,如果是确定NextIndex,得处理回包,然后真正传数据,等下次调用,这样流程比较简单一点
				if len(args.HistoryLogTerm) > 0 {
					rf.NextIndex[server] = reply.NextIndex
				} else if len(args.Entries) > 0 { //表明这次传输是有数据的,需要做回包统计,要更新CommitIndex
					rf.NextIndex[server] += len(args.Entries)

					for j := 0; j < len(args.Entries); j++ {
						rf.Ack[args.Entries[j].Index]++
						if rf.Ack[args.Entries[j].Index] > len(rf.peers)/2 {
							fmt.Printf("peer:%d, log commited\n", rf.me)
							fmt.Println(args.Entries[j])

							var msg ApplyMsg
							msg.Command = args.Entries[j].Command
							msg.CommandValid = true
							msg.CommandIndex = args.Entries[j].Index + 1

							rf.ApplyCh <- msg

							fmt.Printf("peer:%d, debug %d %d %d %d \n", rf.me, args.Entries[j].Term, rf.CurrentTerm, args.Entries[j].Index, rf.CommitIndex)

							//这地方,只有自己新提交了日志,才能更新CommitIndex
							if args.Entries[j].Term == rf.CurrentTerm {
								if args.Entries[j].Index > rf.CommitIndex {
									rf.CommitIndex = args.Entries[j].Index
									fmt.Printf("peer:%d, refresh commitindex to %d\n", rf.me, rf.CommitIndex)
								}

								//rf.StartWork = true
							}
						}
					}
				}
			} else {
				if reply.Term > rf.CurrentTerm {
					rf.CurrentTerm = reply.Term
					rf.State = 1

					rf.persist()
				}
			}
			fmt.Printf("peer:%d,send append success, append loop:%d, send append to %d\n", rf.me, args.LeaderLoopRound, server)

			break
		} else { //收包失败---这个库超时多大
			fmt.Printf("peer:%d,send append failed, append loop:%d, send append to %d,now:%d,retry\n", rf.me, args.LeaderLoopRound, server, time.Now().UnixNano()/1000000)
		}
	}

}

func (rf *Raft) AppendLoop(server int) {

	for i := 0; ; i++ {
		if rf.Stop == true {
			fmt.Printf("peer:%d, append loop stop\n", rf.me)
			break
		}

		//fmt.Printf("peer:%d,append loop:%d\n", rf.me, args.LeaderLoopRound)

		//如果leader被append或是vote解胡,那么leaderLoop要停掉
		//看起来没有变为candidate的可能
		if rf.State == 1 {
			break
		}

		rf.LeaderAppend(server)

		//大哥你睡这一会能保证创造的这几个routine都已经死了吗?
		//跟下边的选举汇总routine类似,多个世代rpc消息交杂的问题
		time.Sleep(time.Duration(rf.SendAppendInterval * 100000))
		rf.AppendLoopRound[server]++
	}
}

//你想想看,如果没有高任期号抹平机制,你leader状态我咋把你搞掉
func (rf *Raft) LeaderLoop() {
	fmt.Printf("peer:%d,start leader loop\n", rf.me)

	for j := 0; j < len(rf.peers); j++ {
		if j != rf.me {
			go rf.AppendLoop(j)
		}

	}
}

//被调方处理投票请求
//
// example RequestVote RPC handler.
//
//大家首先选自己,第一轮铁定超时
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	fmt.Printf("peer:%d,recv voterequest from %d, his term:%d, my term:%d\n", rf.me, args.CandidateId, args.Term, rf.CurrentTerm)
	// Your code here (2A, 2B).
	if args.Term > rf.CurrentTerm {

		//加安全性条件
		if len(rf.Log) > 0 {
			if args.LastLogTerm < rf.Log[len(rf.Log)-1].Term ||
				(args.LastLogTerm == rf.Log[len(rf.Log)-1].Term && args.LastLogIndex < len(rf.Log)-1) {
				reply.VoteGranted = false
			}
		}

		//如果我此时是leader,那么要停掉leader循环

		rf.State = 1
		fmt.Printf("peer:%d,case 1\n", rf.me)

		reply.VoteGranted = true
		rf.VoteFor = args.CandidateId

		rf.CurrentTerm = args.Term
		rf.LastRecvAppendMs = time.Now().UnixNano() / 1000000

		rf.persist()

		//此处是不是要开始主loop?
		go rf.Loop()

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
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// 超时处理函数?
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
	//理解有点问题,不是调用对方的某个函数吗?这里怎么是调用自己的投票?
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//此处的难点在于逐个收到回复的过程中,会不会掺杂着些其他东西
//同步问题...
//给多个server发消息,在中间被截胡了的情况
func (rf *Raft) GoLeaderElection(server int, args *RequestVoteArgs) {
	fmt.Printf("peer:%d,toserver:%d, in GoLeaderElection\n", rf.me, server)

	var reply RequestVoteReply

	ok := rf.sendRequestVote(server, args, &reply)
	if ok {
		fmt.Printf("peer:%d,toserver:%d, sendRequestVote success\n", rf.me, server)
		fmt.Printf("peer:%d,reply:%t,%d\n", rf.me, reply.VoteGranted, reply.Term)
		//做收包处理
		//对方投我
		if reply.VoteGranted {
			rf.VoteMe++

			//要查看,中途被搞成主或者变主之后又变成了follower,这个判断还是否有效?---就说说candidate状态也就持续开始的一小段时间,之后就是leader和follower的天下了

			//要不要对自己发起的选举轮次进行标识,别让多次选举混了---先等等---可能要对收包具体情况做整理,比如只能收同一sb一个包
			if rf.VoteMe >= len(rf.peers)/2+1 && rf.State == 3 {
				//这部分其实就是个leader初始化的代码部分,要抽象出来

				//用状态作为判断标准,进行卡位
				rf.State = 2
				//成为leader,将超时重选时间设置为无穷大
				rf.StartVoteInterval = 2000000000000000

				for i := 0; i < len(rf.peers); i++ {
					rf.MatchIndex = append(rf.MatchIndex, -1)
					rf.NextIndex = append(rf.NextIndex, -1)
					rf.Ack = append(rf.Ack, -1)
				}

				rf.StartWork = false
				//得设置个标识,不能跑多个append routine
				fmt.Printf("peer:%d,become leader\n", rf.me)

				rf.persist()

				go rf.LeaderLoop()
			}
		} else {
			//要做判断哈,也可能是日志问题引起的被拒绝---被拒绝的原因有几条?
			//一种恶心情况,都已经被选为主了,却有收到一个拒绝回复,这种情况可能出现吗?---不要假设,要确信对方一定会出现
			//没有必要做身份判断,因为只要以来这种拒绝,直接我就变成follower
			if reply.Term > rf.CurrentTerm {
				//看看要不要更新我投票中的任期数值--我不必要更新任期,那个有更高任期的家伙会给我发请求把我灭了--Trem值我是收到的
				//论文里说要更新--不再选了,所以没有下一轮更新任期的问题了
				rf.CurrentTerm = reply.Term
				//这个事情诡异了,按论文的说法,我还得变成follower?--可以分类想,也可以统一想--这种情况下也可以,毕竟都看到别的候选人有更大的任期,我没必要再选了,大不了超时了我再重新启动选举
				//变成follower完全没任何问题,因为我根本不管谁当leader,收到append我记录就完了
				//这个地方看看跟后边的总结语句对应下,要做修改
				rf.State = 1

				rf.persist()
			}
		}
		//上述两种状态是不需要汇总的,但不能重复进入
	} else { //rpc失败
		fmt.Printf("sendRequestVote fail\n")
	}

	rf.RPCTimes++
	//全部完成---这段应该只跑一次---最初投自己的的话,这地方也需要改
	if rf.RPCTimes == len(rf.peers)-1 {
		//如果没有选出主,既我不是,也没人通知我他是,重置超时时间--这步判断处理了上边2中情况的问题
		if rf.State == 3 {
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			//rf.LoopVoteInterval = (int64)(200 + r.Intn(200))
			rf.LoopVoteInterval = (int64)(r.Intn(100))
			fmt.Printf("peer:%d,recv over,need election again,need sleep %dms\n", rf.me, rf.LoopVoteInterval)
			time.Sleep(time.Duration(rf.LoopVoteInterval * 1000000))
			fmt.Printf("peer:%d,wake up\n", rf.me)
			go rf.startLeaderElection()
		}
	}
}

//要处理发起选举后,其他机器高任期打击成follower后,停止选举的特殊情况---得到汇总函数里去处理了---这没法处理,函数开始就把标识消掉了
func (rf *Raft) startLeaderElection() {
	fmt.Printf("peer:%d,in startLeaderElection\n", rf.me)

	//被截胡了
	if rf.State == 1 {
		fmt.Printf("peer:%d,meet big term to become follower\n", rf.me)
		return
	}

	rf.CurrentTerm += 1
	rf.VoteMe = 1 //最初投自己这事给忽略了
	rf.VoteFor = rf.me

	rf.persist()

	var args RequestVoteArgs

	args.Term = rf.CurrentTerm
	args.CandidateId = rf.me
	if len(rf.Log) > 0 {
		args.LastLogIndex = len(rf.Log) - 1
		args.LastLogTerm = rf.Log[len(rf.Log)-1].Term
	} else {
		args.LastLogIndex = -1
		args.LastLogTerm = -1
	}

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.GoLeaderElection(i, &args)
		}
	}

	//自然结束,汇总工作交给GoLeaderElection
}

//主循环,查看follower状态下是否收append超时---控制好,启动选举routine后就不能再跑了
//此处的情况是这样,我虽然在别人的选举中应答了别人,但是还没收到append的时候,我就开始恰好开始选举了,所以在回复选举票的时候,应该吧LastRecv更新
func (rf *Raft) Loop() {

	for i := 0; ; i++ {
		if rf.Stop == true {
			fmt.Printf("peer:%d, main loop stop\n", rf.me)
			break
		}

		nowMs := time.Now().UnixNano() / 1000000
		diff := nowMs - rf.LastRecvAppendMs

		fmt.Printf("peer:%d,loop:%d, now:%d, lastrecv:%d,diff:%d,interval:%d\n", rf.me, rf.LoopRound, nowMs, rf.LastRecvAppendMs, diff, rf.StartVoteInterval)

		//state条件是后加的,你在选举状态时是不能再次启动选举的
		if diff > rf.StartVoteInterval && rf.State == 1 {
			fmt.Printf("peer:%d,startLeaderElection\n", rf.me)
			fmt.Printf("peer:%d,lastrecv:%d\n", rf.me, rf.LastRecvAppendMs)
			fmt.Printf("peer:%d,now:%d\n", rf.me, nowMs)

			rf.State = 3 //先做个简单防御,这样routine切换过程中,我就可以发现中间被截胡了

			go rf.startLeaderElection()
			break
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

//开始协议并立即返回?那客户端怎么知道你成功没有?
//参数搞成什么形式呢
//问题是tester怎么测试我是否达成共识了呢?
//真是宽松,都说没保证会提交到日志中,都比啊
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	fmt.Printf("peer:%d,in start\n", rf.me)

	index := -1
	term := -1
	isLeader := false

	//被第一段最后一句话吓的
	if rf == nil {
		return index, term, isLeader
	}

	//也可能处于sb候选人状态哦
	if rf.State != 2 {
		fmt.Printf("peer:%d,not leader\n", rf.me)
		return index, term, isLeader
	}
	fmt.Printf("peer:%d,can log\n", rf.me)
	fmt.Printf("peer:%d,log content:", rf.me)
	pstr := reflect.ValueOf(command)
	fmt.Println(pstr)

	// Your code here (2B).
	//塞到内存结构里就行了
	var item LogItem
	item.Command = command
	item.Index = len(rf.Log)
	item.Term = rf.CurrentTerm
	rf.Log = append(rf.Log, item) //append定期检查的时候查看是否需要发送部分日志---是不是得看看这个操作有没有用

	rf.PrintLog()

	rf.persist()

	rf.StartWork = true

	//index = len(rf.Log) - 1
	index = len(rf.Log) //暂时兼容下
	term = rf.CurrentTerm
	isLeader = true

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
	fmt.Printf("peer:%d killed\n", rf.me)
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
	rf.me = me

	rf.ApplyCh = applyCh

	for i := 0; i < len(peers); i++ {
		rf.AppendLoopRound[i] = 0
	}

	// Your initialization code here (2A, 2B, 2C).
	//raft结构成员初始化
	rf.Stop = false

	rf.CurrentTerm = 0
	rf.VoteFor = -1
	rf.CommitIndex = -1

	//初始状态为follower
	rf.State = 1

	//按照没秒10次append计算
	rf.SleepInterval = 50
	rf.StartVoteInterval = 120
	//LoopVoteInterval

	rf.SendAppendInterval = 50

	//作为candidate时的选举状态
	rf.VoteMe = 0
	rf.RPCTimes = 0

	//append相关
	rf.LastRecvAppendMs = time.Now().UnixNano() / 1000000

	//调试相关
	rf.LoopRound = 0
	rf.LeaderLoopRound = 0

	//一个问题,我还没超时,收到你的投你的请求,该怎么处理?--应该忽略--有种情况是我知道主已死,但我还没到发请求的超时时间--事实证明我该变成follower
	//认定主已死算要超时,循环发起投票也是超时,这里区别?--我怎么忘了为啥后者要比前者大的原因了?--论文列没区别,你自己搞出的区别
	//没有统一的整齐划一的大家一起重新开始超时
	//发append没写--在选举期间收到append也没写--投票和append的混杂,处理好
	//follower回不了包也要启动选举?在这个环境里咋操作啊...这里没法搞回包的是否完成判断啊...

	fmt.Printf("peer:%d,lastRecv:%d\n", rf.me, rf.LastRecvAppendMs)

	// initialize from state persisted before a crash
	//persister是你传过来的,我上次跑的内容你都帮我存着呢
	rf.readPersist(persister.ReadRaftState())

	go rf.Loop()

	return rf
}
