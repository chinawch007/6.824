package shardmaster

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 0

func DPrintf(logHead string, format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		fmt.Printf("%v", logHead)
		fmt.Printf(format, a...)
	}
	return
}

func DPrintln(a ...interface{}) (n int, err error) {
	if Debug > 0 {
		fmt.Println(a...)
	}
	return
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	configs []Config // indexed by config num

	maxraftstate int // snapshot if log grows this big

	SnapshotPersister *raft.Persister
	mapCh             chan bool //当raft收到快照时,回复给server一个消息,server清理map,装载快照

	logHead string
}

type Op struct {
	// Your data here.
	Args interface{}
	Op   string //函数的名字
	Ts   int64
}

func (sm *ShardMaster) isLeader() bool {
	_, isLeader := sm.rf.GetState()

	return isLeader
}

func (sm *ShardMaster) printConfigs() {
	DPrintf(sm.logHead, "masterserver:%d configs:\n", sm.me)
	for _, c := range sm.configs {
		fmt.Println(c)
	}
}

//开始时shard的默认值都是0, 会产生bug
func (sm *ShardMaster) balance() {
	//c := sm.configs[len(sm.configs)-1] //你这么写能改你妹啊

	if len(sm.configs[len(sm.configs)-1].Groups) == 0 {
		for i := 0; i < len(sm.configs[len(sm.configs)-1].Shards); i++ {
			sm.configs[len(sm.configs)-1].Shards[i] = 0
		}
		return
	}

	c := &sm.configs[len(sm.configs)-1]

	s := len(c.Shards) / len(c.Groups)
	y := len(c.Shards) % len(c.Groups)

	var t int
	var b int

	if y == 0 {
		t = s
		b = len(c.Groups)
	} else {
		t = s + 1
		b = y
	}

	var d []int
	count := make(map[int]int) //必须是个map啊,因为那边的gid不一定是几呢?
	for i, _ := range c.Groups {
		count[i] = 0
	}

	countb := 0
	for i, g := range c.Shards { //削峰
		if count[g] == 0 {
			d = append(d, i)
			continue
		}

		if count[g]+1 > t {
			d = append(d, i)
		} else if count[g]+1 == t { //防止超柱
			if countb+1 > b {
				d = append(d, i)
			} else {
				count[g]++
				countb++
			}
		} else {
			count[g]++
		}
	}

	di := 0
	for i, v := range count { //填平
		if v < s {
			for j := 0; j < s-v; j++ {
				DPrintf(sm.logHead, "masterserver:%d,%d from %d to %d\n", sm.me, d[di], c.Shards[d[di]], i)
				c.Shards[d[di]] = i
				di++
				count[i]++
			}
		}
	}

	for i, v := range count {
		if v == s {
			if di >= len(d) {
				break
			}
			DPrintf(sm.logHead, "masterserver:%d,%d from %d to %d\n", sm.me, d[di], c.Shards[d[di]], i)
			c.Shards[d[di]] = i
			di++
			count[i]++
		}
	}

	DPrintf(sm.logHead, "masterserver:%d,after balance:", sm.me)
	for _, g := range c.Shards {
		DPrintf(sm.logHead, "%d,", g)
	}
	DPrintf(sm.logHead, "\n")

}

/*
func (sm *ShardMaster) restoreConfigGroups() {

	var configs []Config
	var ret []Config
	l := len(sm.configs)

	configs = append(configs, sm.configs[l-1])

	var c Config
	var d Config

	for i := l - 2; i >= 0; i-- {
		c = configs[i+2-l] //作为groups的基准,索引别搞错了
		d = sm.configs[i]  //shards是准的,groups是不准的,要修改
		switch sm.ops[i].Op {
		case "Join":
			{
				args := sm.ops[i].Args.(JoinArgs)
				DPrintf(sm.logHead, "masterserver:%d debug:\n", sm.me)
				for _, c := range configs {
					fmt.Println(c)
				}
				for k, _ := range args.Servers {
					delete(c.Groups, k)
				}

				d.Groups = c.Groups
				configs = append(configs, d)

			}
		case "Leave":
			{
				args := sm.ops[i].Args.(LeaveArgs)
				for _, gid := range args.GIDs {
					c.Groups[gid] = sm.groups[gid]
				}

				d.Groups = c.Groups
				configs = append(configs, d)
			}
		default:
			{
				d.Groups = c.Groups
				configs = append(configs, d)
			}

		}
	}

	DPrintf(sm.logHead, "masterserver:%d debug:\n", sm.me)
	for _, c := range configs {
		fmt.Println(c)
	}

	for i := l - 1; i >= 0; i-- {
		ret = append(ret, configs[i])
	}

	sm.configs = ret
}
*/

func (sm *ShardMaster) CommitOp(op Op) {
	DPrintf(sm.logHead, "masterserver:%d, commitop:", sm.me)
	fmt.Println(op)

	if op.Op == "Join" { //只涉及到group,还没涉及到shard,没给这个group分配shard呢
		args := op.Args.(JoinArgs)
		DPrintf(sm.logHead, "masterserver:%d, args:", sm.me)
		fmt.Println(args)

		var c Config
		c.Num = len(sm.configs)
		c.Shards = sm.configs[len(sm.configs)-1].Shards
		c.Groups = make(map[int][]string)
		for k, v := range sm.configs[len(sm.configs)-1].Groups {
			c.Groups[k] = v
		}

		for k, v := range args.Servers { //保证都是原来没有的group
			c.Groups[k] = v
		}

		sm.configs = append(sm.configs, c)
		sm.balance()
		sm.printConfigs()
	} else if op.Op == "Leave" {
		args := op.Args.(LeaveArgs)
		DPrintf(sm.logHead, "masterserver:%d, args:", sm.me)
		fmt.Println(args)

		var c Config
		c.Num = len(sm.configs)
		c.Shards = sm.configs[len(sm.configs)-1].Shards
		c.Groups = make(map[int][]string)
		for k, v := range sm.configs[len(sm.configs)-1].Groups {
			c.Groups[k] = v
		}

		for _, gid := range args.GIDs {
			delete(c.Groups, gid)
		}

		sm.configs = append(sm.configs, c)
		sm.balance()
		sm.printConfigs()
	} else if op.Op == "Move" { //都说是wal,在这里先写日志有什么好处呢?
		args := op.Args.(MoveArgs)
		DPrintf(sm.logHead, "masterserver:%d, args:", sm.me)
		fmt.Println(args)
		c := sm.configs[len(sm.configs)-1]
		c.Num = len(sm.configs)
		c.Shards[args.Shard] = args.GID //这就算完了

		//这地方实际上要开始分布式事务了,要驱动2个Group做数据迁移
		//对于出发方,可以给client返回稍后重试的标识---相比于挂起请求,这样处理简单些
		//对于接收方,加锁操作数据
		//这是一个连调rpc啊
		//master全程加锁情况下,其实master修改配置和成员迁移数据的先后顺序是无所谓的
		//其实不是的,你现在讨论的顺序是master日志,迁移数据,master改配置
		//---按照客户端的要求,成员应该尽早停服,所以在master写日志之前,成员应该先迁移完日志,其实成员此时已经停服了
		//------此时如果用户从成员查不到数据,转而到master来,正好,master正停服呢

		//配置变更要放到最后,等下边成员server把数据都迁移好了之后才搞
		sm.configs = append(sm.configs, c)
		sm.balance()
		sm.printConfigs()

	} else if op.Op == "Query" {
		//do nothing
	} else if op.Op == "QueryAll" {
		//do nothing
	}

	DPrintf(sm.logHead, "masterserver:%d commit\n", sm.me)
}

func (sm *ShardMaster) processRaft(op Op, err *Err) {

	sm.rf.Start(op, op.Ts)

	for {
		var m raft.ApplyMsg
		select {
		case m = <-sm.applyCh:
			{
				DPrintf(sm.logHead, "masterserver:%d CommandValid:%t, CommandIndex:%d,", sm.me, m.CommandValid, m.CommandIndex)
				fmt.Println(m.Command)

				if m.CommandValid {
					sm.CommitOp(m.Command.(Op))

				} else {
					//超时了,实际可能是提交了的,然后我这边怎么处理呢?---get一下,看看是不是设定的值
					DPrintf(sm.logHead, "masterserver:%d,exception \n", sm.me)
				}

				if m.Command.(Op).Ts == op.Ts { //直接判等编译不过,只能这样
					*err = OK
					return
				}
			}
		case <-time.After(300 * time.Millisecond):
			{
				*err = ErrRaftTimeout
				return
			}
		}
	}
}

//4种操作要搞成同一接口吗?---先写出一个试试看
//要对4种操作,每种操作的特殊性做个标识
//The new configuration should divide the shards as evenly as possible among the full set of groups,
//and should move as few shards as possible to achieve that goal.---要包含平衡数据的步骤吗?看测试吧
func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	DPrintf(sm.logHead, "masterserver:%d Join start\n", sm.me)
	DPrintln(args)

	sm.mu.Lock()
	defer sm.mu.Unlock()

	if !sm.isLeader() {
		reply.WrongLeader = true
		DPrintf(sm.logHead, "masterserver:%d Join not leader, return\n", sm.me)
		return
	}

	var op Op
	op.Op = "Join"
	op.Ts = args.Ts
	op.Args = *args

	if args.Retry {
		if sm.rf.SearchLog(op, op.Ts) == raft.AlreadyCommited {
			reply.Err = AlreadyCommited
			return
		} else if sm.rf.SearchLog(op, op.Ts) == raft.NotCommited {
			//此条日志在下层的状态还没有确定,将来要么是被覆盖,要么是提交
			//这种情况下需要让client端到其他机器上继续重试
			reply.Err = NotCommited
			return
		}
	}

	sm.processRaft(op, &reply.Err)

	DPrintf(sm.logHead, "masterserver:%d, join over, reply msg:", sm.me)
	DPrintln(reply)
}

//跟join一样写了条日志改了下配置就完了吗
//可能leave一个group后又有请求去到那个group吗?---不会
//要先把数据迁移走才能删除G吗?
func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	DPrintf(sm.logHead, "masterserver:%d Leave start\n", sm.me)
	DPrintln(args)

	sm.mu.Lock()
	defer sm.mu.Unlock()

	if !sm.isLeader() {
		reply.WrongLeader = true
		DPrintf(sm.logHead, "masterserver:%d Leave not leader, return\n", sm.me)
		return
	}
	reply.WrongLeader = false

	var op Op
	op.Op = "Leave"
	op.Ts = args.Ts
	op.Args = *args

	if args.Retry {
		if sm.rf.SearchLog(op, op.Ts) == raft.AlreadyCommited {
			reply.Err = AlreadyCommited
			return
		} else if sm.rf.SearchLog(op, op.Ts) == raft.NotCommited {
			//此条日志在下层的状态还没有确定,将来要么是被覆盖,要么是提交
			//这种情况下需要让client端到其他机器上继续重试
			reply.Err = NotCommited
			return
		}

	}

	sm.processRaft(op, &reply.Err)

	DPrintf(sm.logHead, "masterserver:%d, leave over, reply msg:", sm.me)
	DPrintln(reply)
}

//应该是把参数中的Shard签到参数中的Gid中
//A Join or Leave following a Move will likely un-do the Move, since Join and Leave re-balance.
//---join和leave自带ballance?
func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	DPrintf(sm.logHead, "masterserver:%d move start\n", sm.me)
	DPrintln(args)

	sm.mu.Lock()
	defer sm.mu.Unlock()

	if !sm.isLeader() {
		reply.WrongLeader = true
		DPrintf(sm.logHead, "masterserver:%d Move not leader, return\n", sm.me)
		return
	}

	var op Op
	op.Op = "Move"
	op.Ts = args.Ts
	op.Args = *args

	if args.Retry {
		if sm.rf.SearchLog(op, op.Ts) == raft.AlreadyCommited {
			reply.Err = AlreadyCommited
			return
		} else if sm.rf.SearchLog(op, op.Ts) == raft.NotCommited {
			//此条日志在下层的状态还没有确定,将来要么是被覆盖,要么是提交
			//这种情况下需要让client端到其他机器上继续重试
			reply.Err = NotCommited
			return
		}

	}

	//***此处是与其他函数功能不同之处,要先让成员把数据迁移完再说***

	sm.processRaft(op, &reply.Err)

	DPrintf(sm.logHead, "masterserver:%d, move over, reply msg:", sm.me)
	DPrintln(reply)
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	//这个锁跟raft的锁没关系,你加了raft也可能不是leader了,只能get完之后检查下
	DPrintf(sm.logHead, "masterserver:%d query start\n", sm.me)
	DPrintln(*args)

	sm.mu.Lock()
	defer sm.mu.Unlock()

	if !sm.isLeader() {
		reply.WrongLeader = true
		DPrintf(sm.logHead, "masterserver:%d query not leader, return\n", sm.me)
		return
	}
	reply.WrongLeader = false

	var op Op
	op.Op = "Query"
	op.Ts = args.Ts
	op.Args = *args
	fmt.Println(op)

	sm.processRaft(op, &reply.Err)
	if reply.Err != OK {
		return
	}

	if args.Num >= len(sm.configs) || args.Num == -1 {
		reply.Config = sm.configs[len(sm.configs)-1]
	} else {
		reply.Config = sm.configs[args.Num]
		//不能返回,你找不到新主或许能找到
	}

	DPrintf(sm.logHead, "masterserver:%d, query over, reply msg:", sm.me)
	DPrintln(reply)
}

//shardserver每次收到请求拉取最新配置,进而根据需要去其他group拉数据
func (sm *ShardMaster) QueryAll(args *QueryAllArgs, reply *QueryAllReply) {
	// Your code here.
	DPrintf(sm.logHead, "masterserver:%d queryall start\n", sm.me)
	DPrintln(*args)

	sm.mu.Lock()
	defer sm.mu.Unlock()

	if !sm.isLeader() {
		reply.WrongLeader = true
		DPrintf(sm.logHead, "masterserver:%d queryall not leader, return\n", sm.me)
		return
	}
	reply.WrongLeader = false

	var op Op
	op.Op = "QueryAll"
	op.Ts = args.Ts
	op.Args = *args
	fmt.Println(op)

	sm.processRaft(op, &reply.Err)
	if reply.Err != OK {
		return
	}

	reply.Configs = sm.configs

	DPrintf(sm.logHead, "masterserver:%d, queryall over, reply msg:", sm.me)
	DPrintln(reply)
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
//没有maxraftstate,没有切片相关的代码
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	//这里是每生成一个新版本的配置,数组多增加一项的意思吗?
	//---看文档的意思到就是这样
	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})
	labgob.Register(QueryAllArgs{})
	/*
		labgob.Register(OpJoin{})
		labgob.Register(OpLeave{})
		labgob.Register(OpMove{})
		labgob.Register(OpQuery{})
	*/
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.mapCh = make(chan bool)
	logHead := "masterserver" + strconv.Itoa(me)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh, nil, logHead)

	// Your code here.
	go sm.RecvRaftMsgRoutine()

	return sm
}

func (sm *ShardMaster) RecvRaftMsgRoutine() {
	for {

		//LOOP:
		var m raft.ApplyMsg
		select {
		case m = <-sm.applyCh:
			{
				sm.mu.Lock()
				DPrintf(sm.logHead, "masterserver:%d, follower get msg from raft CommandValid:%t, CommandIndex:%d\n", sm.me, m.CommandValid, m.CommandIndex)
				DPrintln(m)

				if m.CommandValid {
					sm.CommitOp(m.Command.(Op))
				} else {
					DPrintf(sm.logHead, "masterserver:%d, raft msg invalid\n", sm.me)
				}

				sm.mu.Unlock()
				//goto LOOP //因为是缓冲chan,如果这条之后还有,继续抽
			}
		default:
			{
				//break
				time.Sleep(time.Duration(50 * 1000000))
			}
		}
		//time.Sleep(time.Duration(50 * 1000000))
	}
}
