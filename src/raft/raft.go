package raft

//A的目的是实现心跳和选主
//添加状态
//2a的第二个reElection,是把主阻塞掉,什么也不干,看剩下的两台机器选主.但后来把另一个主页停了,有啥意义是?
//关于主"阻塞了",选出新主,过会新主死了,旧主活了的情况,要用原有协议而不另外添加机制的话,追加日志是可以追加的,但是在分发的时候会被拒绝
//现象陈述,2代主回来之后,3代主一直没给他发append?这个们自己似乎已经取消主的身份,但又选举成功了?
//2代主回来以后,lastRecvTime没有刷新,发起选主,这个是问题吗?---其实是可以刷新的,即遇到高任期号可以刷新下,但这里其实也不影响
//主超时要不要自杀?---在backup中就遇到了这个问题,被网络分区了要不要自杀
//
//4被选为leader
//写日志0
//1 2 3断连
//4接收垃圾日志0到49---问题,那几个sb断连了,给他们发送东西的routine在干什么?----这几个sb的loop在干嘛,loop是单独go出来的routine,应该不会阻塞
//1和3的loop还在跑
//1开始发起选举了 3开始发起选举 2死哪去了
//0 4断连,4的loop狂刷,那之前干啥去了
//1成为新主,接收有用日志50到99---这阶段0的loop都哪去了
//把2停掉,2作为other,作为日志权威
//1接收垃圾日志
//13关掉,04回来,按预期这时候应该是2被选为主,毕竟它的日志任期号大
//4因为当过主,超时时间无限
//2始终没有loop---突然4向2发送append,明显被降维了,于是4出来选主了---4发起2次选主,没能区别开?第一次并没有收到所有的成员回复--0也出来选了
//--部分投票回复收不到的情况,恐怕也要当成总数到达机器数处理
//2之前发起过一次投票,0和4没有回复,到现在才收到失败回复---拒绝别人投票,也要把任期值提升下
//
//
//我这边投出票后,没有重置超时时间
//加日志,选举世代,跳到新loop
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
	//LoopVoteInterval  int64 //作为candidate多久发起一次投票

	SendAppendInterval int64 //主多久发一波append

	//作为candidate时的选举状态
	VoteMe        []int //投我的有几个人
	RPCTimes      []int //已进行多少次RPC,无论成功失败
	ElectionTimes int

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

	LeaderLoopTimes int
	LeaderLoopStop  [50]bool
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
	fmt.Printf("peer:%d,recv append from %d,AppendLoopRound:%d\n", rf.me, args.LeaderId, args.LeaderLoopRound)

	if args.Term < rf.CurrentTerm {
		reply.Success = false
		reply.Term = rf.CurrentTerm
		fmt.Printf("peer:%d,refuse append from %d,AppendLoopRound:%d, his term: %d,my term:%d\n", rf.me, args.LeaderId, args.LeaderLoopRound, args.Term, rf.CurrentTerm)
	} else if args.Term == rf.CurrentTerm { //这地方同选举的地方有点混淆,这个判断需要看日志吗?
		reply.Success = true
		//在任期相等的情况下,也要成为follower---如果收到另一个leader的同等任期append,难道也要变么?漏洞太多---要分情况讨论
		if rf.State == 3 {
			fmt.Printf("peer:%d,from candidate to follower\n", rf.me)
			rf.State = 1
			//go rf.Loop()---大哥,我如果是candidate的话原来就有loop啊...---后边随便看到这句,candidate是没有loop的---怎么又搞不清楚了,为什么不给个loop呢?
			fmt.Printf("peer:%d,candidate recv append from %d, trans to follower, AppendLoopRound:%d\n", rf.me, args.LeaderId, args.LeaderLoopRound)
			go rf.Loop()
		} else if rf.State == 1 {
			fmt.Printf("peer:%d,refresh lastrecvtime\n", rf.me)
			rf.LastRecvAppendMs = time.Now().UnixNano() / 1000000
		}

		//新主首次append,应该要确定NextIndex
		if len(args.HistoryLogTerm) > 0 {
			//reply.NextIndex = rf.getNextIndex(args.HistoryLogTerm)
			fmt.Printf("peer:%d, my nextindex is %d\n", rf.me, reply.NextIndex)

			//要做截断,保证我的日志长度同主机保存的NextIndex一致---无条件截断是否有问题,在本任期还没有输入日志的情况下
			rf.Log = rf.Log[:reply.NextIndex]
			reply.NextIndex = rf.getNextIndex(args.HistoryLogTerm)
		} else {

			for i := 0; i < len(args.Entries); i++ {
				//可能是追加也可能是覆盖---都已经截断了就不会出现覆盖的情况了
				if args.Entries[i].Index >= len(rf.Log) {
					rf.Log = append(rf.Log, args.Entries[i])
				} else {
					rf.Log[args.Entries[i].Index] = args.Entries[i]
				}
			}

			reply.NextIndex = len(rf.Log)

			fmt.Printf("peer:%d, recv log:\n", rf.me)
			for i := 0; i < len(args.Entries); i++ {
				fmt.Print(reflect.ValueOf(args.Entries[i].Command))
				fmt.Printf(":")
			}
			fmt.Printf("\n")

			rf.persist()

			rf.PrintLog()
		}

		//必须先有NextIndex才能确定CommitIndex
		//NextIndex必然大于CommitIndex
		oldCommitIndex := rf.CommitIndex

		if args.LeaderCommit > rf.CommitIndex {
			if len(rf.Log)-1 < args.LeaderCommit {
				rf.CommitIndex = len(rf.Log) - 1
			} else {
				rf.CommitIndex = args.LeaderCommit
			}
			fmt.Printf("peer:%d, refresh commitindex from %d to %d\n", rf.me, oldCommitIndex, rf.CommitIndex)
		}

		for i := oldCommitIndex + 1; i <= rf.CommitIndex; i++ {
			var msg ApplyMsg
			msg.Command = rf.Log[i].Command
			msg.CommandValid = true
			msg.CommandIndex = rf.Log[i].Index + 1

			rf.ApplyCh <- msg

			fmt.Printf("peer:%d, reply to tester\n", rf.me)
			fmt.Println(msg)
		}

		//如果此时有选举,应该使其停止,跟之前的投票期间截胡是一个原理,在发起投票和投票汇总函数中做防御性处理---???
	} else { //事实上这条大任期号强制性原则创造了很多你之前想像不到的情形---保持状态不动,还是follower
		rf.CurrentTerm = args.Term
		fmt.Printf("peer:%d, recv big term append from %d, %d, %d \n", rf.me, args.LeaderId, rf.CurrentTerm, args.Term)

		if rf.State == 2 || rf.State == 3 {
			fmt.Printf("peer:%d, recv big term append from %d, start loop \n", rf.me, args.LeaderId)
			go rf.Loop()
		}
		rf.State = 1                                 //把这条s加上吧,感觉总会有我想不到的角落情形---哦,担心比如不是follower的状态收到这条信息
		rf.StartVoteInterval = 100 + rf.getRand(200) //后加的,但似乎还没引起bug

		rf.persist()

		//这地方是我加的,被截胡后,更新最后接收时间
		rf.LastRecvAppendMs = time.Now().UnixNano() / 1000000

		//一般情况下肯定是第一种
		if len(args.HistoryLogTerm) > 0 {
			reply.NextIndex = rf.getNextIndex(args.HistoryLogTerm)
			fmt.Printf("peer:%d, meet big term, my nextindex is %d\n", rf.me, reply.NextIndex)
		} else {
			fmt.Printf("peer:%d, wrong condition\n", rf.me)

			for i := 0; i < len(args.Entries); i++ {
				rf.Log[args.Entries[i].Index] = args.Entries[i]
			}

			reply.NextIndex = len(rf.Log)

			fmt.Printf("peer:%d, recv log:\n", rf.me)
			for i := 0; i < len(args.Entries); i++ {
				fmt.Print(reflect.ValueOf(args.Entries[i].Command))
				fmt.Printf(":")
			}
			fmt.Printf("\n")

			rf.persist()

			rf.PrintLog()
		}
		//如果此时有选举,应该使其停止,跟之前的投票期间截胡是一个原理,在发起投票和投票汇总函数中做防御性处理
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntries, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) LeaderAppend(server int, leaderLoopTimes int) {
	var argsEntity AppendEntries
	args := &argsEntity

	args.Term = rf.CurrentTerm
	args.LeaderId = rf.me
	args.LeaderLoopRound = rf.AppendLoopRound[server]
	args.LeaderCommit = rf.CommitIndex

	fmt.Printf("peer:%d,in GoLeaderAppend, append loop:%d, send append to %d\n", rf.me, args.LeaderLoopRound, server)
	var reply AppendEntriesReply

	var ok bool
	startLogIndex := -1

	//首先要定位到从哪条开始推
	if rf.NextIndex[server] == -1 { //不单独搞个rpc了,复用append的,简单点
		fmt.Printf("peer:%d, start to determine %d's nextindex\n", rf.me, server)
		for j := 0; j < len(rf.Log); j++ {
			args.HistoryLogTerm = append(args.HistoryLogTerm, rf.Log[j].Term)
		}
	} else {
		startLogIndex = rf.NextIndex[server]
		fmt.Printf("peer:%d, sb:%d %d\n", rf.me, server, rf.NextIndex[server])
		//上边还是用,但这边意义就没有了
		//args.PrevLogIndex
		//args.PrevLogTerm

		if rf.StartWork {
			fmt.Printf("peer:%d,to %d, debug:%d,%d\n", rf.me, server, startLogIndex, len(rf.Log))
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

		if rf.State != 2 {
			fmt.Printf("peer:%d, to %d, already not leader, quit leader loop routine\n", rf.me, server)
			break
		}

		if rf.LeaderLoopStop[leaderLoopTimes] {
			fmt.Printf("peer:%d, leader loop times:%d stop, quit routine \n", rf.me, leaderLoopTimes)
			break
		}

		fmt.Printf("peer:%d, ready to send to %d\n", rf.me, server)

		ok = rf.sendAppendEntries(server, args, &reply)

		fmt.Printf("peer:%d, leader append recv\n", rf.me)
		fmt.Println(reply)

		//收包成功
		if ok {
			if reply.Success {
				if len(args.HistoryLogTerm) > 0 {
					rf.NextIndex[server] = reply.NextIndex
				} else if len(args.Entries) > 0 { //表明这次传输是有数据的,需要做回包统计,要更新CommitIndex
					//rf.NextIndex[server] += len(args.Entries)---这地方问题很大,因为存在截断的情况,不能是递增的
					rf.NextIndex[server] = reply.NextIndex
					fmt.Printf("peer:%d, success send log, incr nextindex, %d, %d\n", rf.me, rf.NextIndex[server], len(args.Entries))
					for j := 0; j < len(args.Entries); j++ {
						fmt.Println(args.Entries[j])
						rf.Ack[args.Entries[j].Index]++
						fmt.Printf("peer:%d, ack %d, %d\n", rf.me, args.Entries[j].Index, rf.Ack[args.Entries[j].Index])
						if rf.Ack[args.Entries[j].Index] > len(rf.peers)/2 {
							fmt.Printf("peer:%d, log commited\n", rf.me)
							fmt.Println(args.Entries[j])

							var msg ApplyMsg
							msg.Command = args.Entries[j].Command
							msg.CommandValid = true
							msg.CommandIndex = args.Entries[j].Index + 1 //兼容哈

							rf.ApplyCh <- msg

							fmt.Printf("peer:%d, reply to tester\n", rf.me)
							fmt.Println(msg)

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
					go rf.Loop() //之前都没写
					rf.StartVoteInterval = 100 + rf.getRand(200)
					fmt.Printf("peer:%d, recv append reply, meet big term,trans to follower, start loop\n", rf.me)

					rf.persist()
				}
			}
			fmt.Printf("peer:%d,send append success, append loop:%d, send append to %d\n", rf.me, args.LeaderLoopRound, server)

			break
		} else { //收包失败---这个库超时多大---老久了,直接堵死了
			fmt.Printf("peer:%d,send append failed, append loop:%d, send append to %d,now:%d,retry\n", rf.me, args.LeaderLoopRound, server, time.Now().UnixNano()/1000000)
			break //醉了,上边一层已经是无限循环了
		}
	}

}

func (rf *Raft) AppendLoop(server int, leaderLoopTimes int) {
	fmt.Printf("peer:%d, start append routine to %d\n", rf.me, server)
	for i := 0; ; i++ {
		if rf.Stop == true {
			fmt.Printf("peer:%d, append loop stop\n", rf.me)
			break
		}

		//如果leader被append或是vote解胡,那么leaderLoop要停掉
		//看起来没有变为candidate的可能
		if rf.State == 1 {
			fmt.Printf("peer:%d, from leader to follower, stop loop\n", rf.me)
			break
		}

		if rf.LeaderLoopStop[leaderLoopTimes] {
			fmt.Printf("peer:%d, leader loop times:%d stop \n", rf.me, leaderLoopTimes)
			break
		}

		rf.LeaderAppend(server, leaderLoopTimes)

		//跟下边的选举汇总routine类似,多个世代rpc消息交杂的问题---旧世代的routine一定要停下了才行,加state判断
		time.Sleep(time.Duration(rf.SendAppendInterval * 100000))
		rf.AppendLoopRound[server]++
	}
}

//你想想看,如果没有高任期号抹平机制,你leader状态我咋把你搞掉
func (rf *Raft) LeaderLoop() {
	rf.LeaderLoopTimes++
	fmt.Printf("peer:%d,start leader loop, times:%d\n", rf.me, rf.LeaderLoopTimes)

	for j := 0; j < len(rf.peers); j++ {
		if j != rf.me {
			go rf.AppendLoop(j, rf.LeaderLoopTimes)
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

		//这地方还是没搞清楚,你哪怕是拒绝了,也要接受降维打击
		//加安全性条件
		/*
			if len(rf.Log) > 0 {
				fmt.Printf("peer:%d,voteargs:", rf.me)
				fmt.Println(args)
				fmt.Printf("peer:%d,my log:%d %d\n", rf.me, rf.Log[len(rf.Log)-1].Term, len(rf.Log)-1)

				if args.LastLogTerm < rf.Log[len(rf.Log)-1].Term ||
					(args.LastLogTerm == rf.Log[len(rf.Log)-1].Term && args.LastLogIndex < len(rf.Log)-1) {
					reply.VoteGranted = false
					rf.CurrentTerm = args.Term
					goto reject
				}
			}

		*/
		//如果我此时是leader,那么要停掉leader循环

		//这个意思是我被你降维打击了,但我并步选举你为主---事实上,如果我有更优先的日志,我应该等会开始选主,让我自己当主
		if rf.State == 2 || rf.State == 3 {
			go rf.Loop()
		}

		rf.State = 1
		rf.StartVoteInterval = 100 + rf.getRand(200) //后加的
		fmt.Printf("peer:%d,case 1, start loop\n", rf.me)

		reply.VoteGranted = true
		rf.VoteFor = args.CandidateId

		rf.CurrentTerm = args.Term
		rf.LastRecvAppendMs = time.Now().UnixNano() / 1000000

		if len(rf.Log) > 0 {
			fmt.Printf("peer:%d,voteargs:", rf.me)
			fmt.Println(args)
			fmt.Printf("peer:%d,my log:%d %d\n", rf.me, rf.Log[len(rf.Log)-1].Term, len(rf.Log)-1)

			if args.LastLogTerm < rf.Log[len(rf.Log)-1].Term ||
				(args.LastLogTerm == rf.Log[len(rf.Log)-1].Term && args.LastLogIndex < len(rf.Log)-1) {
				reply.VoteGranted = false
				rf.VoteFor = -1
				//goto reject
			}
		}

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

//如果出现阻塞,这轮发起的选举事实上没有结束,routine死不了,RPC等值会被共用
//如果后来发起过新的选举,那么旧的选举自然而然就没有意义了
func (rf *Raft) GoLeaderElection(server int, electionTimes int, args *RequestVoteArgs) {
	fmt.Printf("peer:%d,toserver:%d, in GoLeaderElection\n", rf.me, server)

	var reply RequestVoteReply

	//这地方循环发送确实没啥必要,大不了别人选上嘛
	ok := rf.sendRequestVote(server, args, &reply)
	if ok {
		fmt.Printf("peer:%d,toserver:%d, sendRequestVote success\n", rf.me, server)
		fmt.Printf("peer:%d,reply:%t,%d\n", rf.me, reply.VoteGranted, reply.Term)
		//做收包处理
		//对方投我
		if reply.VoteGranted {
			rf.VoteMe[electionTimes]++

			//要查看,中途被搞成主或者变主之后又变成了follower,这个判断还是否有效?---就说说candidate状态也就持续开始的一小段时间,之后就是leader和follower的天下了

			//要不要对自己发起的选举轮次进行标识,别让多次选举混了---先等等---可能要对收包具体情况做整理,比如只能收同一sb一个包
			if rf.VoteMe[electionTimes] >= len(rf.peers)/2+1 && rf.State == 3 && electionTimes == rf.ElectionTimes {
				//这部分其实就是个leader初始化的代码部分,要抽象出来

				//用状态作为判断标准,进行卡位
				rf.State = 2
				//成为leader,将超时重选时间设置为无穷大
				rf.StartVoteInterval = 2000000000000000

				//要考虑到多次被选成主的情况
				if len(rf.MatchIndex) == 0 {
					for i := 0; i < len(rf.peers); i++ {
						rf.MatchIndex = append(rf.MatchIndex, -1)
						rf.NextIndex = append(rf.NextIndex, -1)
						//rf.Ack = append(rf.Ack, -1)
						//rf.Ack = append(rf.Ack, 0) //之前咋初始化为-1了
					}

					//上边又搞错了,日志量和机器数量咋能搞在一起呢?
					for i := 0; i < 500; i++ {
						rf.Ack = append(rf.Ack, 0)
					}
				} else {
					for i := 0; i < len(rf.peers); i++ {
						rf.MatchIndex[i] = -1
						rf.NextIndex[i] = -1
					}

					for i := 0; i < 500; i++ {
						rf.Ack[i] = 0
					}
				}

				rf.StartWork = false
				//得设置个标识,不能跑多个append routine
				fmt.Printf("peer:%d,become leader\n", rf.me)

				rf.persist()

				go rf.LeaderLoop()
			}
		} else {
			//一种恶心情况,都已经被选为主了,却有收到一个拒绝回复,这种情况可能出现吗?---不要假设,要确信对方一定会出现
			if reply.Term > rf.CurrentTerm {
				//看看要不要更新我投票中的任期数值--我不必要更新任期,那个有更高任期的家伙会给我发请求把我灭了--Trem值我是收到的
				//论文里说要更新--不再选了,所以没有下一轮更新任期的问题了
				rf.CurrentTerm = reply.Term
				//这个事情诡异了,按论文的说法,我还得变成follower?--可以分类想,也可以统一想--这种情况下也可以,毕竟都看到别的候选人有更大的任期,我没必要再选了,大不了超时了我再重新启动选举
				rf.State = 1
				fmt.Printf("peer:%d,recv vote reply, meet big term, trans to follower, start loop, %d, %d\n", rf.me, rf.CurrentTerm, reply.Term)
				go rf.Loop()

				rf.persist()
			}
		}
		//上述两种状态是不需要汇总的,但不能重复进入
	} else { //rpc失败
		fmt.Printf("peer:%d,toserver:%d,sendRequestVote fail\n", rf.me, server)
	}

	fmt.Printf("peer:%d,debug, %d, %d\n", rf.me, electionTimes, rf.ElectionTimes)
	rf.RPCTimes[electionTimes]++
	//全部完成---这段应该只跑一次---最初投自己的的话,这地方也需要改
	if rf.RPCTimes[electionTimes] == len(rf.peers)-1 {
		//如果没有选出主,既我不是,也没人通知我他是,重置超时时间--这步判断处理了上边2中情况的问题
		if rf.State == 3 {
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			LoopVoteInterval := (int64)(r.Intn(300))
			fmt.Printf("peer:%d,recv over,need election again,need sleep %dms\n", rf.me, LoopVoteInterval)
			time.Sleep(time.Duration(LoopVoteInterval * 1000000))
			fmt.Printf("peer:%d,wake up\n", rf.me)
			//此处需谨慎,也可能是旧的选举好不容易凑一块了,旧的选举消息自然作废了
			if electionTimes == rf.ElectionTimes {
				rf.ElectionTimes++
				fmt.Printf("peer:%d,inrc ElectionTimes, case 2\n", rf.me)
				go rf.startLeaderElection(electionTimes + 1)
			}

		}
	}
}

//要处理发起选举后,其他机器高任期打击成follower后,停止选举的特殊情况---得到汇总函数里去处理了---这没法处理,函数开始就把标识消掉了
func (rf *Raft) startLeaderElection(electionTimes int) {
	fmt.Printf("peer:%d,in startLeaderElection\n", rf.me)

	rf.RPCTimes = append(rf.RPCTimes, 0)
	rf.VoteMe = append(rf.VoteMe, 1)

	//被截胡了
	if rf.State == 1 {
		fmt.Printf("peer:%d,meet big term to become follower\n", rf.me)
		return
	}

	rf.CurrentTerm += 1
	//rf.VoteMe = 1 //最初投自己这事给忽略了
	rf.VoteFor = rf.me //话说这个值好鸡肋,发起投票,肯定投自己
	///rf.RPCTimes = 0 //这句之前没写,bug
	fmt.Printf("peer:%d,push rpctimes\n", rf.me)

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
			go rf.GoLeaderElection(i, electionTimes, &args)
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

			fmt.Printf("peer:%d,inrc ElectionTimes, case 1\n", rf.me)
			rf.ElectionTimes++
			go rf.startLeaderElection(rf.ElectionTimes)
			break //这种模式的问题在于,如果你发起的选举卡住了,就不会再发起选举
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
	//fmt.Printf("peer:%d,in start\n", rf.me)

	index := -1
	term := -1
	isLeader := false

	//被第一段最后一句话吓的
	if rf == nil {
		return index, term, isLeader
	}

	//也可能处于sb候选人状态哦
	if rf.State != 2 {
		//fmt.Printf("peer:%d,not leader\n", rf.me)
		return index, term, isLeader
	}
	//fmt.Printf("peer:%d,can log\n", rf.me)
	fmt.Printf("peer:%d,log content:", rf.me)
	pstr := reflect.ValueOf(command)
	fmt.Println(pstr)

	// Your code here (2B).
	//塞到内存结构里就行了
	var item LogItem
	item.Command = command
	item.Index = len(rf.Log)
	item.Term = rf.CurrentTerm

	rf.mu.Lock()
	rf.Log = append(rf.Log, item) //append定期检查的时候查看是否需要发送部分日志---是不是得看看这个操作有没有用
	index = len(rf.Log)           //暂时兼容下
	rf.mu.Unlock()

	rf.Ack[len(rf.Log)-1]++ //之前没写,自己应该先投个票,bug

	rf.PrintLog()

	rf.persist()

	rf.StartWork = true

	//index = len(rf.Log) - 1
	//index = len(rf.Log) //暂时兼容下
	term = rf.CurrentTerm
	isLeader = true

	fmt.Printf("peer:%d,return:%d\n", rf.me, index)

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
	rf.StartVoteInterval = 100 + rf.getRand(300)
	//LoopVoteInterval

	rf.SendAppendInterval = 50

	//作为candidate时的选举状态
	//rf.VoteMe = 0
	rf.ElectionTimes = -1

	//append相关
	rf.LastRecvAppendMs = time.Now().UnixNano() / 1000000

	//调试相关
	rf.LoopRound = 0
	rf.LeaderLoopRound = 0

	rf.LeaderLoopTimes = -1

	for i := 0; i < len(rf.LeaderLoopStop); i++ {
		rf.LeaderLoopStop[i] = false
	}

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

func (rf *Raft) getRand(up int) int64 {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	ret := (int64)(r.Intn(up))
	return ret
}

/*
//给外边用要大写,有意思
func (rf *Raft) GetLog(index int) interface{} {
	return rf.Log[index]
}

//每次可以先砍掉4分之3的日志做切片
func (rf *Raft) GetSize() int {
	size := unsafe.Sizeof(rf.Log)

	return (int)(size)
}
*/

func (rf *Raft) stopLeaderLoop(leaderLoopTimes int) {

}

func (rf *Raft) DiscardLog(index int) {
	rf.mu.Lock()

	//你这么截断,日志号没法维护了...
	rf.Log = rf.Log[:index+1]
	rf.persist()

	rf.mu.Unlock()
}
