package raft

//2a的第二个reElection,是把主阻塞掉,什么也不干,看剩下的两台机器选主.但后来把另一个主页停了,有啥意义是?
//关于主"阻塞了",选出新主,过会新主死了,旧主活了的情况,要用原有协议而不另外添加机制的话,追加日志是可以追加的,但是在分发的时候会被拒绝
//现象陈述,2代主回来之后,3代主一直没给他发append?这个们自己似乎已经取消主的身份,但又选举成功了?
//2代主回来以后,lastRecvTime没有刷新,发起选主,这个是问题吗?---其实是可以刷新的,即遇到高任期号可以刷新下,但这里其实也不影响
//主超时要不要自杀?---在backup中就遇到了这个问题,被网络分区了要不要自杀---暂时不用,client server的相互调用,是循环的,这个机器不行就下一个
//
//
//
//我这边投出票后,没有重置超时时间
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
	"math/rand"
	"reflect"
	"sync"
	"time"

	"../labgob"
	"../labrpc"
)

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//不会说遇见无效msg的情况
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
//这地方已经说了快照要用chan的方式传回给上级
//---是说我要复用这个appChan,有必要吗?
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

	State         int //follower1 leader2 candidate3
	Log           []LogItem
	CommitIndex   int //含义变
	SnapshotIndex int //快照压缩进度
	LastLogIndex  int
	LastLogTerm   int
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
	NextIndex []int //下一个要给follower发的日志号,初始化为下一个空日志号
	//MatchIndex      []int //已复制的索引号---有啥用啊,暂时取消
	Ack             []int //leader发follower日志收到多少回复
	RecvdAck        [][10]bool
	NextCommitIndex int //标识我下一个要提交的位置
	//---为了应对情形:刚当上主时有些日志没有提交,但安全性限制让我不能提交,
	//---需要在最新的日志位置先提交个,这个位置就是NextCommitIndex
	//------上边两条理由把我搞迷糊了...再看是因为leader会多次接收到同一index的回复,不能重复提交,所以这个变量是递进的

	StartWork       bool //主专属,安全性限制,只有第一条提交了,才可以给follower推日志
	LeaderLoopTimes int
	LeaderLoopStop  [50]bool //50是估计最大选举轮次的值,用于停掉旧的leader loop

	//调试相关
	LoopRound       int
	LeaderLoopRound int
	AppendLoopRound [10]int

	MapCh chan bool
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
	//DPrintf("peer:%d,in persist\n", rf.me)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.Log)
	e.Encode(rf.VoteFor)
	e.Encode(rf.CommitIndex)
	e.Encode(rf.LastLogIndex)
	e.Encode(rf.LastLogTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)

	//DPrintf("peer:%d,persist over\n", rf.me)
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
	var LastLogIndex int
	var LastLogTerm int
	//压码解码是按特定顺序的吗?---类型?
	if d.Decode(&CurrentTerm) != nil ||
		d.Decode(&Log) != nil ||
		d.Decode(&VoteFor) != nil ||
		d.Decode(&CommitIndex) != nil ||
		d.Decode(&LastLogIndex) != nil ||
		d.Decode(&LastLogTerm) != nil {
		//error...
		DPrintf("peer:%d restore failed\n", rf.me)
	} else {
		rf.CurrentTerm = CurrentTerm
		rf.Log = Log
		rf.VoteFor = VoteFor
		rf.CommitIndex = CommitIndex
		rf.LastLogIndex = LastLogIndex
		rf.LastLogTerm = LastLogTerm
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

	Entries []LogItem
	//PrevLogIndex int
	//PrevLogTerm  int
	LeaderCommit int

	//HistoryLogTerm  []int  //一会看看取消掉吧---取消取消!!!
	//HistoryLogIndex []int  //暂时先这么搞吧...要跟之前的功能兼容
	Snapshot      []byte //新增,向follower发送快照
	SnapshotIndex int    //用于follower做提交时跟CommitIndex作比较的
	MsgType       int    //1表示首次rpc探测follower进度 2表示正常状态传输日志
}

//心跳回包
//要加个能表示需要主发快照的信息
type AppendEntriesReply struct {
	Term    int //当前任期号
	Success bool

	NextIndex int
	//NeedSnapshot bool //临时加下先跑过,看看能不能改成消息类型规范化下
}

//主第一次同follower交流,确定下一个发送位置
//这个函数在日志长度大修改之后就只是主被从交互的一个分支调用了
//---做截断,可以返回个-2,跟leader的NextIndex做个呼应
//------一种特殊情况,主没有快照,从没有日志

//首先你要看它的调用场景,如果你保证,此次rpc后,下次发的肯定是个有效的只包含日志的rpc,那就可以发个>=0的索引回去
//---可以保证,在我这个任期号内,必然只有通过我这个leader的传输你的日志才可能得到增长

//基本假设是传过来的日志肯定是比我要新的

//使用场景确定了,就是初次探测,要确定下一个要发的位置
func (rf *Raft) getNextIndex(recvLog []LogItem) int {
	/*
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
	*/

	/*
		//这条的意思是我的日志同你的日志没有交集
		if rf.Log[len(rf.Log)-1].Index < HistoryLogIndex[0] {
			return -2
		} else { //主从日志有相交的情况
			//确定相交的上下界
			l := rf.Log[len(rf.Log)-1].Index - HistoryLogIndex[0] + 1
			upper := len(rf.Log) - 1
			downer := upper - l + 1
			var i int

			for i = upper; i >= downer; i-- {
				if rf.Log[i].Term == HistoryLogTerm[i-downer] {
					return rf.Log[i].Index + 1 //刚忘了+1
				}
			}

			//如果follower此部分的term不能同leader相匹配,那么同上需要发快照
			if i < downer {
				return -2
			}

			//有匹配的日志点,逻辑跟之前相同
		}

		//为了过编译加的
	*/

	//怎么感觉变得好简单,反正是无条件接收leader的日志,直接就是leader的下一条日志就行了
	//---CommitIndex的下一条
	if len(recvLog) > 0 { //在leader有日志发过来的情况下,可能是到CommitIndex也可能是全部,反正度量都是由leader把控
		return recvLog[len(recvLog)-1].Index + 1
	} else { //那肯定是leader这边快照了
		return rf.SnapshotIndex + 1 //可得保证初始化为-1
	}

	//return 0
}

//followerer处理appendentries的handler
func (rf *Raft) AppendEntries(args *AppendEntries, reply *AppendEntriesReply) {
	DPrintf("peer:%d,recv append from %d,AppendLoopRound:%d\n", rf.me, args.LeaderId, args.LeaderLoopRound)

	if args.Term < rf.CurrentTerm {
		reply.Success = false
		reply.Term = rf.CurrentTerm
		DPrintf("peer:%d,refuse append from %d,AppendLoopRound:%d, his term: %d,my term:%d\n", rf.me, args.LeaderId, args.LeaderLoopRound, args.Term, rf.CurrentTerm)
	} else if args.Term == rf.CurrentTerm { //这地方同选举的地方有点混淆,这个判断需要看日志吗?
		reply.Success = true
		//在任期相等的情况下,也要成为follower---如果收到另一个leader的同等任期append,难道也要变么?---在成员数目确定的情况下,不会出现两台机器同一任期的leader
		rf.mu.Lock()
		if rf.State == 3 { //变成follower之后没有退出继续跑,这次收发就直接开始工作了
			rf.mu.Unlock()
			rf.toFollower(args.Term)
		} else if rf.State == 1 {
			rf.mu.Unlock()
			DPrintf("peer:%d,refresh lastrecvtime\n", rf.me)
			rf.LastRecvAppendMs = time.Now().UnixNano() / 1000000
		}

		//只根据是否有快照来判断是否是初次探测,已经不太准确了,给append消息加个消息类型
		//按照周期性append的方式,探测消息时leader可能是StartWork的方式
		//---所以说我这里要确定,是要接收CommitIndex之前的,还是全部?
		//------是可以无条件接收的,因为CommitIndex之上的日志内容无所谓,都没有提交,是什么都可以,暂时可以用你这个主的内容
		//---------不行的,leader那边要做每条日志的确认统计,所以正常发送必须要从CommitIndex开始
		if args.MsgType == 1 {
			//目标是要确定CommitIndex下一个位置
			reply.NextIndex = rf.getNextIndex(args.Entries)

			//这地方又要强调一个点,只有commit的日志才能压快照,不能收到就压
			//是要比一下吗?看一下上报的方式
			//---快照隐含着你要提交的进度会增多,得包含快照
			if len(args.Snapshot) > 0 && args.SnapshotIndex > rf.SnapshotIndex { //发快照的情形---直接无条件按照leader的快照赋值,还是做下比较?
				//暂时先不考虑follower的快照比leader多的恐怖情形...
				rf.persister.SaveStateAndSnapshot(nil, args.Snapshot)
				//rf.Log = rf.Log[0:0] //这步骤跟快照上报有关系吗
				rf.MapCh <- true

				rf.Log = args.Entries //要么包含CommitIndex或者快照包含它

				if len(args.Entries) > 0 {
					rf.LastLogIndex = args.Entries[len(args.Entries)-1].Index
				} else {
					rf.LastLogIndex = args.SnapshotIndex
				}

				rf.persist()
			} else {
				//这个地方是考虑日志后段部分可能有不匹配的地方
				//在初次探测的步骤中,在还没有StartWork的情况下,最新部分日志是不能强制覆盖的
				//rf.Log = rf.Log[:reply.NextIndex]
				rf.Log = args.Entries
				//此处要设置LastLogIndex
				if len(args.Entries) > 0 {
					rf.LastLogIndex = args.Entries[len(args.Entries)-1].Index
					rf.LastLogTerm = args.Entries[len(args.Entries)-1].Term //这个原来没有,2c最后一个用例跪了
				}

				rf.persist()
				DPrintf("peer:%d, my nextindex is %d\n", rf.me, reply.NextIndex)
			}

		} else { //经过上边的校对,这部分应该只是追加的情形了
			//这样也涵盖了没有日志空心跳的情形
			/*
				for i := 0; i < len(args.Entries); i++ {
					//可能是追加也可能是覆盖---都已经截断了就不会出现覆盖的情况了
					if args.Entries[i].Index >= len(rf.Log) {
						rf.Log = append(rf.Log, args.Entries[i])
					} else {
						rf.Log[args.Entries[i].Index] = args.Entries[i]
					}


				}
			*/

			if len(args.Entries) > 0 {
				//如果上一次发过来的数据我收到,但回包失败,那么此次来的数据可能跟上次到的数据有些重复
				entry0index := rf.GetMatchIndexFromLog(args.Entries[0].Index)
				if entry0index >= 0 && entry0index < len(rf.Log) {
					rf.Log = rf.Log[0:entry0index]
				}

				for i := 0; i < len(args.Entries); i++ {
					rf.Log = append(rf.Log, args.Entries[i])
				}

				//有日志来了要更新这个值,下边NextIndex要用
				rf.LastLogIndex = rf.Log[len(rf.Log)-1].Index
				rf.LastLogTerm = rf.Log[len(rf.Log)-1].Term
				rf.persist()
			}

			reply.NextIndex = rf.LastLogIndex + 1

			DPrintf("peer:%d, recv log:\n", rf.me)
			for i := 0; i < len(args.Entries); i++ {
				DPrint(reflect.ValueOf(args.Entries[i]))
				DPrintf(":")
			}
			DPrintf("\n")

			rf.PrintLog()
		}

		//必须先有NextIndex才能确定CommitIndex
		//NextIndex必然大于CommitIndex
		//---第一次探测NextIndex时这个部分其实是不执行的
		//------会存在一种什么情况说:我之前上报到某个位置,但包含后来的一些日志的快照把这部分都覆盖了,我的上报就没有连续性了
		//---------上报的一部分含义在于是server的table装载日志,但快照部分确实就不需要装载了
		oldCommitIndex := rf.CommitIndex
		/*
			if args.LeaderCommit > rf.CommitIndex {
				if len(rf.Log)-1 < args.LeaderCommit {
					//能确定此时我所有的日志都是该提交的?
					rf.CommitIndex = len(rf.Log) - 1
				} else {
					rf.CommitIndex = args.LeaderCommit
				}
				DPrintf("peer:%d, refresh commitindex from %d to %d\n", rf.me, oldCommitIndex, rf.CommitIndex)
			}
		*/
		rf.CommitIndex = args.LeaderCommit

		//上边是确定此次更新提交的范围,下边是上报提交
		//---允许截断之后逻辑都变了,要先确定当前的提交位置在哪里,然后提交点更新到主的进度
		var startCommit int
		var endCommit int

		//if args.SnapshotIndex > rf.SnapshotIndex {
		if oldCommitIndex == rf.SnapshotIndex {
			//此种情况下是否CommitIndex绝对会碾压
			//所以说能产生快照,要立即产生快照
			//
			startCommit = 0 //从接收的日志的初始位置开始

		} else {
			/*
				for i := 0; i < len(rf.Log); i++ {
					if rf.Log[i].Index == oldCommitIndex {
						startCommit = i + 1
						break
					}
				}
			*/
			startCommit = rf.GetMatchIndexFromLog(oldCommitIndex) + 1
		}
		endCommit = rf.GetMatchIndexFromLog(args.LeaderCommit) //leader发过来的index如果在快照中,这个个返回-1,下边直接不执行了

		for i := startCommit; i <= endCommit; i++ {
			rf.CommitLog(i)
		}

	} else { //对方的任期号比我大
		//我的loop会重启吗?
		rf.toFollower(args.Term)
		DPrintf("peer:%d, recv big term append from %d, %d, %d \n", rf.me, args.LeaderId, rf.CurrentTerm, args.Term)

		//跟上边有段重复的代码
		//---问题是,你给我发的这条append,是有效的还是某条过期的无效的
		if args.MsgType == 1 { //探测阶段
			reply.NextIndex = rf.getNextIndex(args.Entries)
			DPrintf("peer:%d, meet big term, my nextindex is %d\n", rf.me, reply.NextIndex)
		}
	}

}

func (rf *Raft) CommitLog(i int) {
	var msg ApplyMsg
	msg.Command = rf.Log[i].Command
	msg.CommandValid = true
	msg.CommandIndex = rf.Log[i].Index + 1 //兼容哈

	rf.ApplyCh <- msg

	DPrintf("peer:%d, log commited,content:\n", rf.me)
	DPrintln(rf.Log[i].Command)

	DPrintf("peer:%d, reply to tester,content:\n", rf.me)
	DPrintln(msg)
}

//这部分是由leader对达成共识的条目进行提交,不需要考虑快照
//---即不会出现我的CommitIndex在快照中的情况
//------无论是自己生成的快照还是别人传过来的,都肯定都有较大的CommitIndex伴随着
func (rf *Raft) LeaderCommitLog(startIndex int) {

	//因为传进来是有效的,所以这两个值都保证在日志数组索引中
	logStartIndex := rf.GetMatchIndexFromLog(startIndex)
	logOldCommitIndex := rf.GetMatchIndexFromLog(rf.CommitIndex)

	//加这部分是干啥来着?---对StartWork之前日志做提交,这部分在之前可能没有在多数机器上达成共识
	//---此处也要求了,CommitIndex之后的日志是绝不能被快照化的
	//------关键还是说正常流程上不能批量提交,只能逐个提交,所以多条日志要提交肯定是这种情况
	if logStartIndex-logOldCommitIndex > 1 {
		DPrintf("peer:%d, leader first time commit, old:%d, new:%d\n", rf.me, rf.CommitIndex, startIndex)
		for i := logOldCommitIndex + 1; i < logStartIndex; i++ {
			rf.CommitLog(i)
		}
	}

	//一直到日志的末尾?全部?
	//---会出现批量提交的情况吗?注意下
	for i := logStartIndex; i < len(rf.Log); i++ {

		if rf.Ack[i] <= len(rf.peers)/2 {
			break
		}

		//提交,更新CommitIndex
		DPrintf("peer:%d, log commited,content:\n", rf.me)
		DPrintln(rf.Log[i].Command)

		rf.CommitIndex = rf.Log[i].Index
		rf.NextCommitIndex = rf.CommitIndex + 1
		DPrintf("peer:%d, refresh commitindex to %d\n", rf.me, rf.CommitIndex)

		rf.CommitLog(i)
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntries, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//使用场景:1 主在自己的日志中查从的NextIndex所匹配的日志数组的index
//---------2 从提交用作数组区段末尾索引 3 主提交用作数组区段首尾索引 4 主取要发给从的日志,取CommitIndex对应的日志数组index

//要考虑没匹配上,到了日志末尾的情况,即还没有下一个要发的日志
//---这地方是个思维误区啊...你匹配不上难道就可以发end索引吗?不考虑其他情况吗
//------我们可以确定的是,follower传过来的index肯定是小于我leader的CommitIndex的,不可能出现大于的情况,有选主机制保证
//------如果找不到说明 1,过低,在快照里呢 2,过高,我还没收到---过低这部分其实我可以用SnapshotIndex作比对确定
//------结果1不会出现,因为我首次通信就会对其所有快照和日志---不对,可能对齐之后刚好我就生成快照了
//---------这2中情况,我返回日志数组长度,结果都是一个空数组,都是安全的

//-1表示index在快照中 传回日志最大索引+1的值不会引起问题,只是会在下一步造成一个空数组

//你要想想看,经过第一次rpc的试探,事实上,此时这个index,要么对应日志,要么对应快照

//不搞那么麻烦了,直接在快照中-1,正常正常,超过了就数组长度
func (rf *Raft) GetMatchIndexFromLog(index int) int {

	//对应快照的情况,即上次校对后,我这边又生成了快照
	if index <= rf.SnapshotIndex {
		return -1
	}

	var i int
	for i = 0; i < len(rf.Log); i++ {
		if rf.Log[i].Index == index {
			break
		}
	}
	return i

}

//再次强调下,leaderLoopTimes是要传参数,不能用rf成员的,编程错误:混淆变量名称
//第一次发送先确定NextIndex情况,下一轮再根据NextIndex看发快照还是发日志
//---改了,第一次发全量日志和切片,判断具体进度
func (rf *Raft) AppendToFollower(server int, leaderLoopTimes int) {
	var argsEntity AppendEntries
	args := &argsEntity
	args.Term = rf.CurrentTerm
	args.LeaderId = rf.me
	//明显有的节点会卡住在某round
	args.LeaderLoopRound = rf.AppendLoopRound[server]
	args.LeaderCommit = rf.CommitIndex

	DPrintf("peer:%d,in AppendToFollower, append loop round:%d, send append to %d\n", rf.me, args.LeaderLoopRound, server)

	var reply AppendEntriesReply

Snap:

	//加入快照之后这个应该是全局的索引了,不再是当前的log数组中的索引了
	if rf.NextIndex[server] == -1 { //默认值是-1,确定follower的具体进度,全量发快照和日志,一次性搞定
		DPrintf("peer:%d, send all log to determine %d's nextindex\n", rf.me, server)
		args.MsgType = 1

		args.Entries = rf.Log
		//args.Entries = rf.Log[:rf.getMatchIndexFromLog(rf.CommitIndex)+1]
		args.Snapshot = rf.persister.ReadSnapshot()
	} else {
		args.MsgType = 2

		startLogIndex := rf.GetMatchIndexFromLog(rf.NextIndex[server])
		DPrintf("peer:%d, to server:%d's nextindex: %d, startindex:%d\n", rf.me, server, rf.NextIndex[server], startLogIndex)

		//follower跟我断连了好久,我这边都已经生成快照了
		//---可得保证这种情况下返回的是-1
		if startLogIndex == -1 {
			rf.NextIndex[server] = -1
			goto Snap
		}

		if rf.StartWork {
			//这种区段用法,不需要检查数组是否有内容吗?---看日志是可以的,打出来区段长度为0
			args.Entries = rf.Log[startLogIndex:len(rf.Log)]
		} else if rf.CommitIndex >= rf.NextIndex[server] {
			//在新主继任,还没startWork的情况下,也是可以传播并让follower提交一部分日志的
			//---再看此处时,深感细节精悍
			//---此处的安全性讨论,CommitIndex确实有可能在快照中,startLogIndex会更小,反着来的话,数组为空
			args.Entries = rf.Log[startLogIndex : rf.GetMatchIndexFromLog(rf.CommitIndex)+1]
		}

		DPrintf("peer:%d,to %d, send log index start from:%d, len:%d\n", rf.me, server, startLogIndex, len(args.Entries))
		for i := 0; i < len(args.Entries); i++ {
			DPrint(reflect.ValueOf(args.Entries[i]))
			DPrintf(":")
		}
		DPrintf("\n")
	}

	ok := rf.sendAppendEntries(server, args, &reply)
	DPrintf("peer:%d, leader append recv from server:%d, loopround:%d\n", rf.me, server, args.LeaderLoopRound)
	DPrintln(reply)

	//收包成功
	if ok {
		if reply.Success {
			rf.NextIndex[server] = reply.NextIndex

			if len(args.Entries) > 0 { //表明这次传输是有数据的,需要做回包统计,要更新CommitIndex
				//发日志牵涉到共识,暂时发快照只会在第一次探测rpc中存在,之后follower会自己进行快照生成

				DPrintf("peer:%d, success send log, refresh nextindex, %d, %d\n", rf.me, rf.NextIndex[server], len(args.Entries))
				//主是不能读快照的,所以向上层server提交一定要保证完备
				for j := 0; j < len(args.Entries); j++ {
					DPrintln(args.Entries[j])
					//ack长度可得整够了
					if rf.RecvdAck[args.Entries[j].Index][server] {
						continue
					}
					rf.RecvdAck[args.Entries[j].Index][server] = true

					rf.mu.Lock() //防止并发提交同一日志,使上报2次

					rf.Ack[args.Entries[j].Index]++
					DPrintf("peer:%d, incr log index:%d, ack:%d\n", rf.me, args.Entries[j].Index, rf.Ack[args.Entries[j].Index])
					//TestFailNoAgree2B里边测出来,这里会有后边的日志先提交的现象?
					if rf.Ack[args.Entries[j].Index] > len(rf.peers)/2 &&
						args.Entries[j].Index == rf.NextCommitIndex {
						rf.LeaderCommitLog(args.Entries[j].Index)
					}

					rf.mu.Unlock() //防止并发提交同一日志,使上报2次

				}

			}
		} else {
			if reply.Term > rf.CurrentTerm {
				rf.toFollower(reply.Term)

				DPrintf("peer:%d, recv append reply from:%d, meet big term,trans to follower, start loop\n", rf.me, server)

			}
		}
		DPrintf("peer:%d,send append success, append loop:%d, send append to %d\n", rf.me, args.LeaderLoopRound, server)
	} else {
		//收包失败---这个库超时多大---老久了,直接堵死了
		DPrintf("peer:%d,send append failed, append loop round:%d, send append to %d,now:%d,retry\n", rf.me, args.LeaderLoopRound, server, time.Now().UnixNano()/1000000)
	}

}

//对单个节点定时发append消息
//times的层级是高过round
func (rf *Raft) AppendLoop(server int, leaderLoopTimes int) {
	DPrintf("peer:%d, start append routine to %d\n", rf.me, server)
	for {

		if rf.Stop == true {
			DPrintf("peer:%d, append loop stop\n", rf.me)
			break
		}

		if rf.LeaderLoopStop[leaderLoopTimes] {
			DPrintf("peer:%d, leader loop times:%d stop \n", rf.me, leaderLoopTimes)
			break
		}

		go rf.AppendToFollower(server, leaderLoopTimes)

		//后边少写个0,造成频繁append每过TestCount
		//我咋记得是200来着???
		time.Sleep(time.Duration(rf.SendAppendInterval * 1000000))
		rf.AppendLoopRound[server]++
	}
}

//你想想看,如果没有高任期号抹平机制,你leader状态我咋把你搞掉
//一对多,每个follower一个routine
func (rf *Raft) LeaderLoop() {

	DPrintf("peer:%d,start leader loop, times:%d\n", rf.me, rf.LeaderLoopTimes)

	for j := 0; j < len(rf.peers); j++ {
		if j != rf.me {
			go rf.AppendLoop(j, rf.LeaderLoopTimes)
		}

	}
}

//如果我更新或相同返回false,否则true
//如果选票过来了,对方赢返回true,那么就是说对方至少和我一样返回true
func (rf *Raft) CompareLog(args *RequestVoteArgs) bool {
	//我有日志,比较最后日志的任期和日志长度
	/*
		if len(rf.Log) > 0 {
			DPrintf("peer:%d,voteargs:", rf.me)
			DPrintln(args)
			DPrintf("peer:%d,my log:%d %d\n", rf.me, rf.Log[len(rf.Log)-1].Term, len(rf.Log)-1)

			if args.LastLogTerm < rf.Log[len(rf.Log)-1].Term ||
				(args.LastLogTerm == rf.Log[len(rf.Log)-1].Term && args.LastLogIndex < len(rf.Log)-1) {
				return true
			} else {
				return false
			}
		} else { //这地方逻辑做了个大简化,我只要没有日志,对方肯定赢
			return false
		}
	*/

	DPrintf("peer:%d,voteargs:", rf.me)
	DPrintln(args)
	DPrintf("peer:%d,my log:%d %d\n", rf.me, rf.LastLogTerm, rf.LastLogIndex)
	//考虑下初始状态下没日志的情况
	//---接收方只有日志进度优于投票发起方的情况下才会返回拒绝
	if args.LastLogTerm < rf.LastLogTerm ||
		(args.LastLogTerm == rf.LastLogTerm && args.LastLogIndex < rf.LastLogIndex) {
		return false
	} else { //相等情况下也会返回true,即赞同
		return true
	}

}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	DPrintf("peer:%d,recv voterequest from %d, his term:%d, my term:%d\n", rf.me, args.CandidateId, args.Term, rf.CurrentTerm)
	// Your code here (2A, 2B).
	biggerTerm := (args.Term > rf.CurrentTerm)
	if biggerTerm {
		//考虑当前可能的几种状态
		rf.toFollower(args.Term)
	}

	//此处有问题啊...你变成follower之后任期会被拉高的...
	bLog := rf.CompareLog(args)

	//跟之前想的不同,同任期的情况下也选它
	//---同任期只能是挑战者自己增长的,不是隔离自增的话,就是同期大家竞争选主,但这种情况对方votefor是不会为空的,此种情形可忽略
	//---votefor可表示是否是candidate

	//votefor条件,投别人就不能投你了
	if biggerTerm && bLog && rf.VoteFor == -1 {
		DPrintf("peer:%d,return granted\n", rf.me)
		reply.VoteGranted = true
		rf.VoteFor = args.CandidateId
		//在刚初始情况下,我是follower状态,不重置这个,一会也要发起选举了
		rf.LastRecvAppendMs = time.Now().UnixNano() / 1000000
		rf.persist()
		//怎么又回想起不同任期双主的问题了...
	} else {
		DPrintf("peer:%d,case 3\n", rf.me)
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
	DPrintf("peer:%d,toserver:%d, in GoLeaderElection, electionTimes:%d\n", rf.me, server, electionTimes)

	var reply RequestVoteReply

	//发一次就够了,这地方循环发送确实没啥必要,大不了别人选上嘛---说的好
	ok := rf.sendRequestVote(server, args, &reply)
	if ok {
		DPrintf("peer:%d,toserver:%d, sendRequestVote success\n", rf.me, server)
		DPrintf("peer:%d,reply vote:%t,term:%d\n", rf.me, reply.VoteGranted, reply.Term)

		//对方投我
		if reply.VoteGranted {
			rf.VoteMe[electionTimes]++

			//要查看,中途被搞成主或者变主之后又变成了follower,这个判断还是否有效?---就说说candidate状态也就持续开始的一小段时间,之后就是leader和follower的天下了
			//因为是并发routine,此时实例状态确实可能已经变成了leader或follower,选举世代也可能有变化
			rf.mu.Lock()
			if rf.VoteMe[electionTimes] >= len(rf.peers)/2+1 && rf.State == 3 && electionTimes == rf.ElectionTimes {
				rf.mu.Unlock()
				rf.toLeader()
			} else {
				rf.mu.Unlock() //忘写了,把别的回复确认堵死了
			}
			//rf.mu.Unlock() //忘写了,把别的回复确认堵死了---直接写到外边死的真惨
		} else {
			//一种恶心情况,都已经被选为主了,却有收到一个拒绝回复,这种情况可能出现吗?---不要假设,要确信对方一定会出现
			if reply.Term > rf.CurrentTerm { //这个强制性动作其实跟state和选举世代都没有关系
				rf.toFollower(reply.Term)

				DPrintf("peer:%d,recv vote reply, meet big term, trans to follower, start loop, %d, %d\n", rf.me, rf.CurrentTerm, reply.Term)
			}
		}
		//上述两种状态是不需要汇总的,但不能重复进入
		//另外还有一种平票拒绝的情况
	} else {
		//rpc失败
		DPrintf("peer:%d,toserver:%d,sendRequestVote rpc fail\n", rf.me, server)
	}

	//消息全收到的时候,做二次发起选举的准备
	//---原论文是超时发起第二次,你这样可能会出现没收到某些机器消息而超时但没有发起选举
	/*
		rf.RPCTimes[electionTimes]++
		if rf.RPCTimes[electionTimes] == len(rf.peers)-1 {
			//超时时间其实不必重置,初始化的时候随机下以后一直用就行了
			if rf.State == 3 {
				LoopVoteInterval := rf.getRand(400)
				DPrintf("peer:%d,recv over,need election again,need sleep %dms\n", rf.me, LoopVoteInterval)
				time.Sleep(time.Duration(LoopVoteInterval * 1000000))
				DPrintf("peer:%d,wake up\n", rf.me)

				if electionTimes == rf.ElectionTimes {
					rf.ElectionTimes++
					//DPrintf("peer:%d,inrc ElectionTimes, case 2, to: %d\n", rf.me, rf.ElectionTimes)
					rf.RPCTimes = append(rf.RPCTimes, 0)
					rf.VoteMe = append(rf.VoteMe, 1)
					//DPrintf("peer:%d,push rpctimes\n", rf.me)
					go rf.startLeaderElection(electionTimes + 1)
				}

			}
		}
	*/
}

func (rf *Raft) startLeaderElection() {

	DPrintf("peer:%d,in startLeaderElection\n", rf.me)

	rf.mu.Lock()

	//只有可能从3变到1,所以只考虑这一种情况了.发生了并发,在其他的routine被改变成了1
	if rf.State == 1 {
		DPrintf("peer:%d,in startLeaderElection meet big term to become follower\n", rf.me)
		rf.mu.Unlock()
		return
	}

	rf.mu.Unlock()

	var args RequestVoteArgs
	args.Term = rf.CurrentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = rf.LastLogIndex
	args.LastLogTerm = rf.LastLogTerm

	//还是做接力,多个routine,都尝试总结
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.GoLeaderElection(i, rf.ElectionTimes, &args)
		}
	}

	//睡一会
	LoopVoteInterval := 500 + rf.getRand(500)
	DPrintf("peer:%d,wait for recv msg,need sleep %dms\n", rf.me, LoopVoteInterval)
	time.Sleep(time.Duration(LoopVoteInterval * 1000000))
	DPrintf("peer:%d,wake up\n", rf.me)

	//自然结束,汇总工作交给GoLeaderElection
	//---改了,这个routine成为了candidate的专属,需要睡眠检查是否选主成功
	//从原来想单个成员发消息的routine的总结代码copy过来的
	rf.mu.Lock()
	if rf.State != 3 {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	//这里自己创建自己,不搞无限循环了
	//---自己变自己,会有点重复动作
	DPrintf("peer:%d,last time not make a leader, vote again\n", rf.me)
	rf.toCandidate()
}

//主循环,查看follower状态下是否收append超时---控制好,启动选举routine后就不能再跑了
//此处的情况是这样,我虽然在别人的选举中应答了别人,但是还没收到append的时候,我就开始恰好开始选举了,所以在回复选举票的时候,应该把LastRecv更新
//跟上边类似,times级别大于round
func (rf *Raft) Loop() {

	//放外边是为了打日志从0开始,不在StartElection中是因为要做为StartElection的参数
	for {
		if rf.Stop == true {
			DPrintf("peer:%d, main loop stop\n", rf.me)
			break
		}

		nowMs := time.Now().UnixNano() / 1000000
		diff := nowMs - rf.LastRecvAppendMs

		DPrintf("peer:%d,times:%d,loopround:%d, now:%d, lastrecv:%d,diff:%d,interval:%d\n", rf.me, rf.ElectionTimes, rf.LoopRound, nowMs, rf.LastRecvAppendMs, diff, rf.StartVoteInterval)

		//state条件是后加的,你在选举状态时是不能再次启动选举的
		//后边的状态其实不用判断,后边会break,只有是follower状态时才有这个loop
		if diff > rf.StartVoteInterval && rf.State == 1 {
			rf.toCandidate()
			/*

				rf.mu.Lock()
				//做个toCandidate啊
				//---看看还有哪种情况下会做这种转变
				rf.State = 3 //先做个简单防御,这样routine切换过程中,我就可以发现中间被截胡了
				//加次数和推数组放在一起
				//rf.ElectionTimes++
				rf.RPCTimes = append(rf.RPCTimes, 0)
				rf.VoteMe = append(rf.VoteMe, 1)
				//DPrintf("peer:%d,push rpctimes\n", rf.me)
				rf.mu.Unlock()

				//DPrintf("peer:%d,inrc ElectionTimes, case 1, to: %d\n", rf.me, rf.ElectionTimes)

				//routine接力
				//---这地方完全不用起routine啊
				go rf.startLeaderElection(rf.ElectionTimes)
				//要考虑我是候选者,却发起2次选举的问题
				//---因为任期号冲突而没有选出leader,所以重新发起选举是为了增加任期号
			*/
			break
		} else {
			time.Sleep(time.Duration(rf.SleepInterval * 1000000))
		}
		rf.LoopRound++
	}
}

func (rf *Raft) PrintLog() {
	DPrintf("peer:%d,all log:", rf.me)

	for i := 0; i < len(rf.Log); i++ {
		DPrint(reflect.ValueOf(rf.Log[i]))
		DPrintf(":")
	}
	DPrintf("\n")
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
		return index, term, isLeader
	}

	DPrintf("peer:%d,in start log content:", rf.me)
	pstr := reflect.ValueOf(command)
	DPrintln(pstr)

	// Your code here (2B).
	//塞到内存结构里就行了

	rf.mu.Lock()
	var item LogItem
	item.Command = command
	//这个值也只有在这种情况下才会增长
	rf.LastLogIndex = rf.LastLogIndex + 1
	rf.LastLogTerm = rf.CurrentTerm
	item.Index = rf.LastLogIndex
	item.Term = rf.CurrentTerm

	rf.Log = append(rf.Log, item) //append定期检查的时候查看是否需要发送部分日志---是不是得看看这个操作有没有用
	rf.persist()
	index = rf.LastLogIndex + 1
	rf.Ack[rf.LastLogIndex]++ //原来没放到锁范围里,推测会有并发bug
	rf.mu.Unlock()

	//rf.Ack[len(rf.Log)-1]++ //之前没写,自己应该先投个票,bug

	rf.PrintLog()

	//不用等结果,有请求就算正式开始了吗?
	//---你这条不设置成真的,发不了日志,那成了死锁了,21军规
	rf.StartWork = true

	term = rf.CurrentTerm
	isLeader = true

	DPrintf("peer:%d,return the test start value:%d\n", rf.me, index)

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
	DPrintf("peer:%d killed\n", rf.me)

	//follower状态下main loop停掉,leader状态下append loop停掉,candidate状态下直接杀了吧
	rf.Stop = true

	rf.PrintLog()

	time.Sleep(time.Duration(10 * 1000000)) //少睡会
	nowMs := time.Now().UnixNano() / 1000000
	DPrintf("peer:%d kill sleep over,now:%d\n", rf.me, nowMs)

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
	persister *Persister, applyCh chan ApplyMsg, MapCh chan bool) *Raft {
	//DPrintf("peer:%d,make\n", me)
	DPrintf("peer:%d,make\n", me)

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	// initialize from state persisted before a crash
	rf.me = me
	rf.ApplyCh = applyCh
	rf.MapCh = MapCh
	rf.Stop = false

	// Your initialization code here (2A, 2B, 2C).

	//初始状态为follower,可别一上来就candidate发起选举,等会
	rf.State = 1
	//Log
	rf.LastLogIndex = -1 //我以为这个一直都在,怎么没了
	rf.CommitIndex = -1
	rf.SnapshotIndex = -1

	rf.CurrentTerm = 0
	rf.LastRecvAppendMs = time.Now().UnixNano() / 1000000

	//按照没秒10次append计算---发现其实这个没啥意义,太频繁了会超过测试程序的限制
	rf.SleepInterval = 200
	//rf.StartVoteInterval = 300 + rf.getRand(150)
	//这个值会用在初次选举和之后的本times的超时检测
	rf.StartVoteInterval = 500 + rf.getRand(500)

	//文档里说了1s中不能发送超过10次....把代码中与时间有关的地方都做协调性的修改
	//rf.SendAppendInterval = 200
	rf.SendAppendInterval = 200

	rf.VoteFor = -1
	//作为candidate时的选举状态,为了处理bug加的
	rf.ElectionTimes = -1

	//如果不是批量发的话,这俩会有什么不同吗
	for i := 0; i < len(rf.peers); i++ {
		//rf.MatchIndex = append(rf.MatchIndex, -1)
		rf.NextIndex = append(rf.NextIndex, -1)
	}

	for i := 0; i < 2000; i++ {
		rf.Ack = append(rf.Ack, 0)

		var b [10]bool
		for i := 0; i < 10; i++ {
			b[i] = false
		}
		rf.RecvdAck = append(rf.RecvdAck, b)
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

	DPrintf("peer:%d,lastRecv:%d\n", rf.me, rf.LastRecvAppendMs)

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

/*成为leader后提交尴尬部分的日志,看来是没用上
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
*/

//把加锁关系捋顺了
//---看那几个给其他机器发选票的单发rpc routine是否有并发问题
func (rf *Raft) toCandidate() {
	rf.mu.Lock()

	rf.State = 3

	DPrintf("peer:%d,incr term\n", rf.me)
	rf.CurrentTerm += 1
	rf.VoteFor = rf.me
	rf.persist()

	rf.mu.Unlock()

	rf.ElectionTimes++
	//rf.RPCTimes = append(rf.RPCTimes, 0)//最初是为了统计我发起的每一个选举轮次,收到的回复的次数,收完了才开始下一次的选举
	rf.VoteMe = append(rf.VoteMe, 1)
	//rf.mu.Unlock()

	go rf.startLeaderElection()
}

//要不要跟下边一样加个锁?
func (rf *Raft) toLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//用状态作为判断标准,进行卡位
	rf.State = 2
	rf.StartVoteInterval = 2000000000000000

	//要考虑到多次被选成主的情况
	for i := 0; i < len(rf.peers); i++ {
		rf.NextIndex[i] = -1
	}

	rf.NextCommitIndex = rf.LastLogIndex + 1

	//这里重置计数是否合适?
	for i := 0; i < 2000; i++ {
		rf.Ack[i] = 0
		for j := 0; j < 10; j++ {
			rf.RecvdAck[i][j] = false
		}

	}

	for i := 0; i < len(rf.Log); i++ {
		rf.Ack[rf.Log[i].Index]++
	}

	rf.StartWork = false
	DPrintf("peer:%d,become leader\n", rf.me)

	//应该停掉不假,但这个事情不应该拖延到现在才做啊?
	//---放到什么时候做了?
	rf.LeaderLoopTimes++
	/*
		if rf.LeaderLoopTimes > 0 {
			rf.LeaderLoopStop[rf.LeaderLoopTimes-1] = true
		}
	*/

	rf.VoteFor = -1

	rf.persist()

	go rf.LeaderLoop()
}

func (rf *Raft) toFollower(term int) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	//原来是放在if里边的,如果原来状态就是1,这个也要刷新一下,不然的话马上发起选举,用刚刚得到的高term去扰乱秩序
	rf.LastRecvAppendMs = time.Now().UnixNano() / 1000000
	if rf.State == 2 || rf.State == 3 {

		//leader只会向这一个方向转移
		if rf.State == 2 {
			rf.LeaderLoopStop[rf.LeaderLoopTimes] = true
		}

		DPrintf("peer:%d,case 1, start loop\n", rf.me)
		//旧loop删掉了吗
		go rf.Loop()
	}

	rf.State = 1
	rf.CurrentTerm = term
	rf.VoteFor = -1
	rf.persist()
	//rf.StartVoteInterval = 200 + rf.getRand(300)
	rf.StartVoteInterval = 500 + rf.getRand(500)
}

//这边不要由上边设置界限,直接我自己吧CommitIndex之前的都扔了就完了
//这边抛弃日志跟选举,追加是异步并列的\
//---再看下具体所需功能???
func (rf *Raft) DiscardLog(index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//你这么截断,日志号没法维护了...---积累历史清楚索引长度
	//rf.Log = rf.Log[:index+1]

	if index > rf.LastLogIndex {
		rf.Log = rf.Log[0:0]
	}

	if len(rf.Log) > 0 {
		if index < rf.Log[0].Index {
			return
		}

		if index > rf.Log[len(rf.Log)-1].Term {
			rf.Log = rf.Log[0:0]
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

const (
	NotExist        = "NotExist"
	NotCommited     = "NotCommited"
	AlreadyCommited = "AlreadyCommited"
)

//要是有截断的话这个就完全没法用了...
//---考虑下截断的情况---并不是把日志全都抹了,要留一点尾巴用于这个查询
func (rf *Raft) SearchLog(command interface{}) string {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := len(rf.Log) - 1; i >= 0; i-- {
		if command == rf.Log[i].Command {
			if rf.CommitIndex >= rf.Log[i].Index {
				return AlreadyCommited
			} else {
				return NotCommited
			}
		}
	}

	return NotExist
}
