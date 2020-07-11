package shardkv

import (
	"bytes"
	"fmt"
	"strconv"
	"sync"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
	"../shardmaster"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
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

//这个包跟kvraft没有任何包含关系,只是个仿写
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key   string
	Value string
	Op    string
	Ts    int64
	Shard int // 看看哈,现实中会不会变化,即一个key的所属shard?

	TransShards []int                     //用于trans,执行时删除掉相应shard
	Table       map[int]map[string]string //用于pull,执行时加入到总库
	//Configs     []shardmaster.Config      //每次配置变更必有的扩散
	Config shardmaster.Config
	//---相应的配置修改应该都已经在query中做完了?
	//------没做,一起做吧
	//---------follower需不需要最新
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int //一个group负责多个shard
	masters      []*labrpc.ClientEnd
	smClient     *shardmaster.Clerk
	maxraftstate int // snapshot if log grows this big
	Stop         bool

	// Your definitions here.
	Table             map[int]map[string]string
	SnapshotPersister *raft.Persister
	mapCh             chan bool //当raft收到快照时,回复给server一个消息,server清理map,装载快照

	configs []shardmaster.Config //需要像master一样把所有的config版本都记录下来吗?
	valid   []bool               //对于每个配置,是否真的曾拉过数据负责读写,用于判断是否能被其他group拉取

	logHead    string
	configInit bool

	configNow shardmaster.Config
	opTs      []int64
}

func (kv *ShardKV) isLeader() bool {
	_, isLeader := kv.rf.GetState()

	return isLeader
}

func (kv *ShardKV) CommitOp(op Op) {
	if op.Op == "Put" {
		if _, ok := kv.Table[op.Shard]; !ok {
			kv.Table[op.Shard] = make(map[string]string)
		}
		kv.Table[op.Shard][op.Key] = op.Value
		fmt.Printf("shardserver:%v push ts:%d\n", kv.logHead, op.Ts)
		kv.opTs = append(kv.opTs, op.Ts)
		fmt.Printf("shardserver:%v put result:%v\n", kv.logHead, kv.Table[op.Shard][op.Key])
	} else if op.Op == "Append" { //也不知道append的定义是不是这个
		kv.Table[op.Shard][op.Key] += op.Value
		fmt.Printf("shardserver:%v push ts:%d\n", kv.logHead, op.Ts)
		kv.opTs = append(kv.opTs, op.Ts)
		fmt.Printf("shardserver:%v ,append %v result:%v\n", kv.logHead, op.Value, kv.Table[op.Shard][op.Key])
	} else if op.Op == "TransShard" { //类似于get,不需要做什么了
		for _, v := range op.TransShards {
			delete(kv.Table, v)
		}
	} else if op.Op == "RefreshConfig" { //暂时不删数据,先把主体过了
		//kv.configs = append(kv.configs, op.Config)
		kv.configNow = op.Config

		/*
			for i, v := range kv.configs {
				if i > 0 && v.Num != kv.configs[i-1].Num+1 {
					fmt.Printf("shardserver:%v configs not continue\n", kv.logHead)
				}
			}
		*/

		for k1, v1 := range op.Table { //table为空就不做操作了
			//kv.Table[k1] = v1
			if _, ok := kv.Table[k1]; !ok {
				kv.Table[k1] = make(map[string]string)
			}

			for k2, v2 := range v1 {
				kv.Table[k1][k2] = v2
			}
		}

		//kv.persistServerState()
	}

	fmt.Printf("shardserver:%v commit:", kv.logHead)
	fmt.Println(op)
}

func (kv *ShardKV) processRaft(op Op, err *Err) {

	kv.rf.Start(op, op.Ts)
	for {
		var m raft.ApplyMsg

		select {
		case m = <-kv.applyCh:
			{
				DPrintf("shardserver:%v, CommandValid:%t, CommandIndex:%d,", kv.logHead, m.CommandValid, m.CommandIndex)
				DPrintln(m.Command)

				if m.CommandValid {
					kv.CommitOp(m.Command.(Op))
				} else { //可以用来接收其他的消息
					DPrintf("shardserver:%v,exception \n", kv.logHead)
				}

				if m.Command.(Op).Ts == op.Ts {
					*err = OK
					return
				}
			}
		//就是说可能存在连续消息,但只要有一次失败就返回
		//---超时之后消息还会不会来呢
		case <-time.After(300 * time.Millisecond):
			{
				DPrintf("shardserver:%v,in timeout \n", kv.logHead)
				*err = ErrRaftTimeout
				return
			}
		}
	}

}

//寻找路由应该是client那边的功能,路由之后到我这边来,这边不用管
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	fmt.Printf("shardserver:%v get start key:%v\n", kv.logHead, args.Key)
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if !kv.isLeader() {
		reply.WrongLeader = true
		fmt.Printf("shardserver:%v get not leader, return, key:%v\n", kv.logHead, args.Key)
		return
	}
	reply.WrongLeader = false

	//kv.QueryConfig()
	if kv.Stop == true { //遇到过集群停了routine没停的情况
		//得保证stop后有一段时间缓冲,有时间消除这些routine
		fmt.Printf("shardserver:%v Get stop\n", kv.logHead)
		return
	}

	if kv.configNow.Shards[key2shard(args.Key)] != kv.gid {
		reply.Err = WrongGroup
		return
	}

	if args.ConfigNum != kv.configNow.Num {
		reply.Err = "ConfigNumNotMatch"
		fmt.Printf("shardserver:%v confignum not match, args:%d, me:%d\n", kv.logHead, args.ConfigNum, kv.configNow.Num)
		return
	}

	var op Op
	op.Op = "Get"
	op.Key = args.Key
	op.Value = "_"
	op.Ts = args.Ts

	kv.processRaft(op, &reply.Err)
	if reply.Err != OK {
		return
	}

	//这样可能也并不是标准逐条的,因为我可能会一连串提交多条日志
	if reply.Err == OK && kv.rf.Persister.RaftStateSize() > kv.maxraftstate {
		DPrintf("shardserver:%v, RaftStateSize larggeer than the limit: %d, %d \n", kv.logHead, kv.rf.Persister.RaftStateSize(), kv.maxraftstate)
		kv.makeSnapshot(-1)
	}

	val, isExist := kv.Table[args.Shard][args.Key]
	if isExist {
		reply.Value = val
		reply.Err = OK
		DPrintf("shardserver:%v, get exist, key:%v, value:%v, ts:%d\n", kv.logHead, args.Key, val, args.Ts)
	} else {
		reply.Err = KeyNotExist
		DPrintf("shardserver:%v, get not exist, key:%v, ts:%d\n", kv.logHead, args.Key, args.Ts)
	}

	DPrintf("shardserver:%v, get successed, key:%v, value:%v, ts:%d\n", kv.logHead, args.Key, val, args.Ts)
}

func (kv *ShardKV) searchTs(ts int64) bool {
	DPrintf("shardserver:%v:", kv.logHead)
	DPrintln(kv.opTs)

	for _, v := range kv.opTs {
		if v == ts {
			return true
		}
	}

	return false
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	fmt.Printf("shardserver:%v putappend start, key:%v,value:%v\n", kv.logHead, args.Key, args.Value)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	fmt.Printf("shardserver:%v putappend after lock, key:%v,value:%v\n", kv.logHead, args.Key, args.Value)

	if !kv.isLeader() {
		reply.WrongLeader = true
		fmt.Printf("shardserver:%v PutAppend not leader, return, key:%v\n", kv.logHead, args.Key)
		return
	}
	reply.WrongLeader = false

	//fmt.Printf("shardserver:%v putappend before query\n", kv.logHead)
	//kv.QueryConfig()
	//fmt.Printf("shardserver:%v putappend after query:", kv.logHead)
	//fmt.Println(kv.configs)
	if kv.Stop == true { //遇到过集群停了routine没停的情况
		//得保证stop后有一段时间缓冲,有时间消除这些routine
		fmt.Printf("shardserver:%v PutAppend stop\n", kv.logHead)
		return
	}

	//除非初始状态下,不然此处不会没有配置,测试程序里应该都会先搞好配置再读写
	if kv.configNow.Shards[key2shard(args.Key)] != kv.gid {
		reply.Err = WrongGroup
		return
	}

	if args.ConfigNum != kv.configNow.Num {
		reply.Err = "ConfigNumNotMatch"
		fmt.Printf("shardserver:%v confignum not match, args:%d, me:%d\n", kv.logHead, args.ConfigNum, kv.configNow.Num)
		return
	}

	var op Op
	op.Op = args.Op
	op.Shard = args.Shard
	op.Key = args.Key
	op.Value = args.Value
	op.Ts = args.Ts

	if args.Retry {
		//if kv.rf.SearchLog(op, op.Ts) == raft.AlreadyCommited {
		//if l := len(kv.opTs); kv.opTs[l-1] == args.Ts { //已提交情况下
		if kv.searchTs(args.Ts) {
			reply.Err = AlreadyCommited
			return
		} else if kv.rf.SearchLog(op, op.Ts) == raft.NotCommited { //没提交的肯定不会快照截断
			//此条日志在下层的状态还没有确定,将来要么是被覆盖,要么是提交
			//这种情况下需要让client端到其他机器上继续重试
			reply.Err = NotCommited
			return
		}

	}
	fmt.Printf("shardserver:%v putappend before raft\n", kv.logHead)
	kv.processRaft(op, &reply.Err)
	fmt.Printf("shardserver:%v putappend after raft\n", kv.logHead)
	fmt.Printf("shardserver:%v putappend before makesnapshot\n", kv.logHead)
	if reply.Err == OK && kv.rf.Persister.RaftStateSize() > kv.maxraftstate {
		DPrintf("shardserver:%v, RaftStateSize larggeer than the limit: %d, %d \n", kv.logHead, kv.rf.Persister.RaftStateSize(), kv.maxraftstate)
		kv.makeSnapshot(-1)
	}
	fmt.Printf("shardserver:%v putappend after makesnapshot\n", kv.logHead)

	DPrintf("shardserver:%v, putappend over, key:%v,value:%v, reply msg:", kv.logHead, args.Key, args.Value)
	DPrintln(reply)
}

func (kv *ShardKV) RecvRaftMsgRoutine() {
	for {
		if kv.Stop == true { //遇到过集群停了routine没停的情况
			//
			fmt.Printf("shardserver:%v RecvRaftMsgRoutine stop\n", kv.logHead)
			return
		}

	LOOP:
		var m raft.ApplyMsg
		select {
		case m = <-kv.applyCh:
			{
				DPrintf("shardserver:%v, in recv before lock\n", kv.logHead)
				kv.mu.Lock()
				DPrintf("shardserver:%v, follower get msg from raft CommandValid:%t, CommandIndex:%d\n", kv.logHead, m.CommandValid, m.CommandIndex)
				DPrintln(m)

				if m.CommandValid {
					kv.CommitOp(m.Command.(Op))
					kv.rf.MuLock()
					//只要是判断的位置都加了锁,就不会产生快照相互冲突的情况
					//DPrintf("shardserver:%v, RaftStateSize :%d,limit: %d\n", kv.logHead, kv.rf.Persister.RaftStateSize(), kv.maxraftstate)
					if kv.rf.Persister.RaftStateSize() > kv.maxraftstate {
						DPrintf("shardserver:%v, RaftStateSize larggeer than the limit: %d, %d \n", kv.logHead, kv.rf.Persister.RaftStateSize(), kv.maxraftstate)
						kv.makeSnapshot(m.CommandIndex)
					}
					kv.rf.MuUnlock()
				} else {
					DPrintf("shardserver:%v, raft msg invalid\n", kv.logHead)
				}

				kv.mu.Unlock()
				DPrintf("shardserver:%v, in recv after lock\n", kv.logHead)
				goto LOOP //因为是缓冲chan,如果这条之后还有,继续抽
			}
		default:
			{
				break
			}

		}

		time.Sleep(time.Duration(50 * 1000000))
	}
}

//这里不用了吗
func (kv *ShardKV) PutEmptyLog(serverName string) {
	var args PutAppendArgs

	args.Key = "wang"
	args.Value = "chong"
	args.Op = "Put"
	args.Retry = false
	args.Ts = time.Now().UnixNano() / 1000000

	DPrintf("shardserver:%v put start, key: %v, value:%v, op:%v\n", kv.logHead, args.Key, args.Value, args.Op)

	var reply PutAppendReply
	srv := kv.make_end(serverName)
	ok := srv.Call("KVServer.PutAppend", &args, &reply)
	if ok {
		if !reply.WrongLeader {
			if reply.Err == OK {
				DPrintf("shardserver:%v put to %v successed, key: %v, value:%v, op:%v\n", kv.logHead, serverName, args.Key, args.Value, args.Op)
				return
			} else if reply.Err == ErrRaftTimeout { //超时也有可能是成功的吧?
				DPrintf("shardserver:%v put to %v raft timeout, key: %v, value:%v, op:%v\n", kv.logHead, serverName, args.Key, args.Value, args.Op)
			}
		} else {
			DPrintf("shardserver:%v put to %v wrong leader, key: %v, value:%v, op:%v\n", kv.logHead, serverName, args.Key, args.Value, args.Op)
		}
	} else {
		DPrintf("shardserver:%v put to %v rpc error, retry, key: %v, value:%v, op:%v\n", kv.logHead, serverName, args.Key, args.Value, args.Op)
	}

	DPrintf("shardserver:%v empty put fail, key: %v, value:%v, op:%v\n", kv.logHead, args.Key, args.Value, args.Op)
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()

	DPrintf("shardserver:%v final kv:\n", kv.logHead)

	for k, v := range kv.Table {
		DPrintf("shardserver:%v shard:%d:", kv.logHead, k)
		DPrintln(v)
	}

	kv.Stop = true
	// Your code here, if desired.
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
//这地方明确了,界限值是字节数
//但我怎么衡量我raft结构中的字节数呢?
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
//?
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
//之前raft中的普通rpc使用不行吗?
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	if maxraftstate == -1 {
		kv.maxraftstate = 100000000
	} else {
		kv.maxraftstate = maxraftstate
	}
	kv.Stop = false

	//下边3个是shardkv独有的
	kv.make_end = make_end
	kv.gid = gid
	//kv.masters = masters //天然自带到master的client
	kv.logHead = "shardserver_" + strconv.Itoa(gid) + "_" + strconv.Itoa(me)
	kv.smClient = shardmaster.MakeClerk(masters, kv.logHead)

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.mapCh = make(chan bool)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh, kv.mapCh, kv.logHead)

	kv.Table = make(map[int]map[string]string) //int标识shard

	kv.SnapshotPersister = persister

	kv.makeTableFromSnapshot() //没有快照时persister类会返回错误
	//fmt.Println(kv.Table)
	//fmt.Println(kv.rf.Log)
	//fmt.Println(kv.rf.SnapshotIndex)
	//fmt.Println(kv.rf.CommitIndex)
	indexLimit := kv.rf.GetMatchIndexFromLog(kv.rf.CommitIndex) //有意思,改成大写就能外部引用了
	//fmt.Println(indexLimit)
	fmt.Printf("shardserver:%v, logs:", kv.logHead)
	fmt.Println(kv.rf.Log)
	for i := 0; i <= indexLimit; i++ {
		fmt.Println(kv.rf.Log[i].Command.(Op))
		kv.CommitOp(kv.rf.Log[i].Command.(Op))
	}

	kv.configInit = false

	//kv.readPersistServerState(kv.SnapshotPersister.ReadServerState())

	go kv.RecvRaftMsgRoutine()
	go kv.RecvSanpshotRoutine()
	//这个还不能随便个机器都跑这个routine,只能是leader来跑
	//---得引入自杀功能了,减少些麻烦
	//------正常好的leader可别自杀了
	go kv.QueryConfigRoutine() //刚开始咨询的时候master还没启动呢,要不要睡一会?

	fmt.Printf("shardserver:%vstart over\n", kv.logHead)

	return kv
}

func (kv *ShardKV) makeSnapshot(targetIndex int) {

	//加锁?
	//这地方要统一修正下,看看,跟所有机器的快照进度规范化有关
	//你一定要保证,这里是下层raft刚刚提交完日志才进的这个函数,保证这个index是有效的
	//主调这个无论如何都是安全的
	var index int
	if targetIndex == -1 {
		index = kv.rf.GetMatchIndexFromLog(kv.rf.CommitIndex) //提交到哪压到哪,别全压
	} else {
		index = kv.rf.GetMatchIndexFromLog(targetIndex)
	}

	//DPrintf("shardserver:%v, debug: %d, %d\n", kv.logHead, index, kv.rf.CommitIndex)
	//你这个写法其实主从的日志截断位置是不一致的

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	//都忘了快照里还有这俩东西来着
	e.Encode(kv.rf.Log[index].Index)
	e.Encode(kv.rf.Log[index].Term)
	e.Encode(kv.Table)
	//e.Encode(kv.configs)
	//e.Encode(kv.valid) //其实是跟configs同步的,可以用日志生成的
	e.Encode(kv.configNow)
	e.Encode(kv.configInit) //上边都是跟日志相关的,你这个跟日志不相关啊
	DPrintf("shardserver:%v, configinit: %t\n", kv.logHead, kv.configInit)

	data := w.Bytes()

	kv.SnapshotPersister.SaveStateAndSnapshot(nil, data)

	kv.rf.DiscardLog(index)
}

//raft是否需要加锁
func (kv *ShardKV) makeTableFromSnapshot() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	data := kv.SnapshotPersister.ReadSnapshot()

	if data == nil || len(data) < 1 { // bootstrap without any state?
		DPrintf("shardserver:%v, snapshot size error\n", kv.logHead)
		return
	}

	//生成快照的字段,两边要对的上
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var index int //这俩没用上吗?
	var term int
	var table map[int]map[string]string
	//var configs []shardmaster.Config
	//var valid []bool
	var configNow shardmaster.Config
	var configInit bool
	//压码解码是按特定顺序的吗?---类型?
	if d.Decode(&index) != nil ||
		d.Decode(&term) != nil ||
		d.Decode(&table) != nil ||
		//d.Decode(&configs) != nil ||
		//d.Decode(&valid) != nil ||
		d.Decode(&configNow) != nil ||
		d.Decode(&configInit) != nil { //这个table直接这么搞也不知道好不好使

		DPrintf("shardserver:%v, makeTableFromSnapshot failed\n", kv.logHead)
	} else {
		kv.Table = table
		//kv.configs = configs
		//kv.valid = valid
		kv.configNow = configNow
		kv.configInit = configInit
	}

	//DPrintf("shardserver:%v, after makeTableFromSnapshot, table:", kv.logHead)
	//DPrintln(kv.Table)
}

//raft接收到快照后
//这个问题是这样,这个触发效果一定要是非常立即的,决不能被阻塞,不然的话并行操作map会有问题
//所以这地方的危险在于,作为follower,主发来的快照和日志一起来的话,先后顺序没法保证
//---如果limit设置的太小会出现刚载入完这个,接收日志的部分也生成快照
func (kv *ShardKV) RecvSanpshotRoutine() {
	/*
		for {
			//会一直阻塞
			_ = <-kv.mapCh
			DPrintf("shardserver:%v, get snapshot msg from raft\n", kv.logHead)

			//每接到一次就装载一次吗?
			kv.makeTableFromSnapshot()
		}
	*/

	for {
		if kv.Stop == true { //遇到过集群停了routine没停的情况
			//
			fmt.Printf("shardserver:%v RecvSanpshotRoutine stop\n", kv.logHead)
			return
		}

		select {
		case _ = <-kv.mapCh:
			{
				DPrintf("shardserver:%v, get snapshot msg from raft\n", kv.logHead)
				kv.makeTableFromSnapshot()
			}
		default:
			{
				time.Sleep(time.Duration(10 * 1000000))
			}
		}
	}
}

//收到转移通知的时候,就要把相应的shard删掉?
//------这边需不需要做检查配置之类的工作
//收到的shards是超集,要筛选
func (kv *ShardKV) TransShard(args *TransShardArgs, reply *TransShardReply) {
	fmt.Printf("shardserver:%v transshard start\n", kv.logHead)
	fmt.Println(args)
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if !kv.isLeader() {
		reply.WrongLeader = true
		fmt.Printf("shardserver:%v transshard not leader, return\n", kv.logHead)
		return
	}
	reply.WrongLeader = false

	//做configNum的相关检查
	//---这地方事实上要做个限定,所有group的config num从系统初始化就要保持,最多差1
	//---stop集群的时候剩余group还要到该集群去取数据?stop集群不是把他杀死吧?
	//------调用我们写的leave,只是在配置中不存在而已

	//在配置当前一次跳一次的情况下,第一种情况会出现,当某个group配置更新过快,超过2档

	//对方的当前档位比我的大,但它需要的是与他同档位的数据,我提供不了
	if args.ConfigNum > kv.configNow.Num {
		reply.Err = "ConfigNumTooBig"
		fmt.Printf("shardserver:%v meet big config num, my: %d, receive:%d, return\n", kv.logHead, kv.configNow.Num, args.ConfigNum)
		return
	}

	//raft操作会删掉转移的shard,提前
	reply.Table = make(map[int]map[string]string)
	//reply.ConfigNum = make(map[int]int)
	for _, v := range args.Shards {
		reply.Table[v] = make(map[string]string) //要是一直没碰到有效的,就保持为空吧

		for k2, v2 := range kv.Table[v] {
			reply.Table[v][k2] = v2
		}

		//删除
		//delete(kv.Table, v)

	}

	//类似于get,判断连通性
	var op Op
	op.Ts = args.Ts
	op.Op = "TransShard"
	//op.TransShards = args.Shards
	for _, v := range args.Shards {
		op.TransShards = append(op.TransShards, v)
	}

	fmt.Printf("shardserver:%v in transshard before raft\n", kv.logHead)
	kv.processRaft(op, &reply.Err)
	fmt.Printf("shardserver:%v int transshard after raft before makesnapshot\n", kv.logHead)

	if reply.Err == OK && kv.rf.Persister.RaftStateSize() > kv.maxraftstate {
		DPrintf("shardserver:%v, RaftStateSize larggeer than the limit: %d, %d \n", kv.logHead, kv.rf.Persister.RaftStateSize(), kv.maxraftstate)
		kv.makeSnapshot(-1)
	}

	fmt.Printf("shardserver:%v in transshard after makesnapshot\n", kv.logHead)
	/*
		fmt.Printf("shardserver:%v in transshard\n", kv.logHead)
		fmt.Println(kv.Table)
		fmt.Println(kv.valid)
		fmt.Println(kv.configs)
	*/

	DPrintf("shardserver:%v, transshard over, reply msg:", kv.logHead)
	DPrintln(reply)
}

type ChanStruct struct {
	configNum map[int]int               //每个shard的版本
	table     map[int]map[string]string //每个shard的数据
}

//操作类似于get,至于对方配置修改的工作,想办法交个pullShard来做,就不由这个rpc来让对方处理了
func (kv *ShardKV) pullShardRpc(groups map[int][]string, gid int, shards []int, ch chan ChanStruct) {
	fmt.Printf("shardserver:%v pullshardrpc start:", kv.logHead)
	fmt.Println(gid)
	fmt.Println(shards)
	var ret ChanStruct

	defer func() { ch <- ret }()

	var args TransShardArgs
	args.Gid = gid
	args.Shards = shards //统一下命名
	args.FromGroup = kv.gid
	args.Ts = time.Now().UnixNano() / 1000000
	args.ConfigNum = kv.configNow.Num

	for {
		//使用的是当前配置,即旧配置
		//---是这样的,当我不负责某个shard,我不能立即清除,不然其他的group就拉不到了
		servers := groups[gid] //如果我本次查的是新group,自然是查不到的
		for si := 0; si < len(servers); si++ {

			if kv.Stop == true { //遇到过集群停了routine没停的情况
				//得保证stop后有一段时间缓冲,有时间消除这些routine
				fmt.Printf("shardserver:%v pullshardrpc stop\n", kv.logHead)
				return
			}

			srv := kv.make_end(servers[si])
			var reply TransShardReply
			ok := srv.Call("ShardKV.TransShard", &args, &reply)

			if ok {
				if !reply.WrongLeader {
					if reply.Err == OK {
						DPrintf("shardserver:%v pullshardrpc to %v successed\n", kv.logHead, servers[si])
						fmt.Printf("shardserver:%v pullshardrpc return:", kv.logHead)
						fmt.Println(reply)
						//ret.table = reply.Table
						ret.table = make(map[int]map[string]string)
						for k1, v1 := range reply.Table {
							if _, ok := ret.table[k1]; !ok {
								ret.table[k1] = make(map[string]string)
							}

							for k2, v2 := range v1 {
								ret.table[k1][k2] = v2
							}
						}
						ret.configNum = reply.ConfigNum
						return
					} else if reply.Err == ErrRaftTimeout { //超时也有可能是成功的吧?
						DPrintf("shardserver:%v pullshardrpc to %v raft timeout\n", kv.logHead, servers[si])
					} else if reply.Err == "ConfigNumTooBig" {
						DPrintf("shardserver:%v pullshardrpc to %v my configNum is bigger\n", kv.logHead, servers[si])
					}
				} else {
					DPrintf("shardserver:%v pullshardrpc to %v wrong leader\n", kv.logHead, servers[si])
				}
			} else {
				DPrintf("shardserver:%v pullshardrpc to %v rpc error, retry\n", kv.logHead, servers[si])
			}
			if kv.Stop == true { //遇到过集群停了routine没停的情况
				//得保证stop后有一段时间缓冲,有时间消除这些routine
				fmt.Printf("shardserver:%v pullshardrpc stop\n", kv.logHead)
				return
			}
		}
		DPrintf("shardserver:%v pullshardrpc after a loop not successed, need loop again,sleep awhile\n", kv.logHead)
		time.Sleep(time.Duration(1000000000)) //关于睡眠的必要性,在分区的情况下,你不睡的话那就完全空跑了
		DPrintf("shardserver:%v pullshardrpc after a loop not successed, wake up\n", kv.logHead)
	}
}

//目的是拿到一个shrad为key的小table
func (kv *ShardKV) pullShard(groups map[int][]string, gidShard map[int][]int) map[int]map[string]string {
	fmt.Printf("shardserver:%v pullshard start", kv.logHead)
	fmt.Println(gidShard)

	gidChan := make(map[int]chan ChanStruct)
	shardData := make(map[int]map[string]string) //做最终返回的

	for k, _ := range gidShard {
		if len(gidShard[k]) > 0 {
			gidChan[k] = make(chan ChanStruct)
			go kv.pullShardRpc(groups, k, gidShard[k], gidChan[k]) //shard全发过去了
		}
	}

	if kv.Stop == true { //遇到过集群停了routine没停的情况
		//得保证stop后有一段时间缓冲,有时间消除这些routine
		fmt.Printf("shardserver:%v pullshard stop\n", kv.logHead)
		return shardData
	}

	for _, v := range gidChan { //每个chan都要接收
		tmp := <-v
		for k2, v2 := range tmp.table {
			shardData[k2] = v2
		}
	}

	DPrintf("shardserver:%v, pullshard over:", kv.logHead)
	DPrintln(shardData)

	return shardData
}

//历史配置中所有的group配置,有些group退出后就不在新配置中了,但仍要去查询
//---测试里每次变化都是增或删一台机器,就新旧2条配置就够了,不用全量
func (kv *ShardKV) makeGroups(configs []shardmaster.Config) map[int][]string {
	ret := make(map[int][]string)

	for _, v := range configs {
		for k2, v2 := range v.Groups {
			if _, ok := ret[k2]; !ok {
				ret[k2] = v2
			}
		}
	}

	return ret
}

//还没加锁,注意加下锁
//你所说的配置阶梯上升问题在于,有可能出现一个group持有一个很旧的配置,去取下一num配置取不到的问题
//---暂时看test中的例子都是大家一起死机,一起活,并没有某些group死的时候配置变更了,暂时不用考虑某个group跳配置的情况
//------得有个变量表示配置状态是否stable,即我是否能负责当前配置num的读写服务
//---------加锁限定啦
func (kv *ShardKV) QueryConfigRoutine() {
	for {
		if !kv.isLeader() {
			time.Sleep(time.Duration(50 * 1000 * 1000))
			continue
		}

		configNumNow := kv.configNow.Num //初始化为0啦

		c := kv.smClient.Query(configNumNow + 1) //初始这个num要是0

		//这个判断会包含第一次实际拉到空配置的情况,
		//if c.Num == kv.config.Num { //配置结构体不能直接判等
		if c.Num == configNumNow {
			//DPrintf("shardserver:%v, config not change, continue\n", kv.logHead)
			time.Sleep(time.Duration(50 * 1000 * 1000))
			continue
		}
		DPrintf("shardserver:%v, query new config:", kv.logHead)
		DPrintln(c)

		shardData := make(map[int]map[string]string)

		//没初始化没有新旧配置对比
		//---初始状态下我需要去拉去配置,但此时应该没有数据,不用考虑数据的迁移
		//------注意初始无配置情况下请求到来的处理
		//---------因为上边的拦截,这个初始状态,拉到的会是真正的初始配置
		//------------只扩散配置,不扩散数据
		//---------------我第一次拉的是空配置还是1号配置?
		//if !kv.configInit {
		//	kv.configInit = true
		if c.Num == 1 {

		} else {
			//如果需要向别的group迁移数据则迁之
			shard1 := make(map[int]bool)
			shard2 := make(map[int]bool)
			gidShard := make(map[int][]int) //要从哪些gid拉哪些shard的数据

			//分别拿出新旧配置的本群负责的shard
			for i, v := range kv.configNow.Shards {
				if v == kv.gid {
					shard1[i] = true
				}
			}
			for i, v := range c.Shards {
				if v == kv.gid {
					shard2[i] = true
				}
			}

			for k1, _ := range shard2 { //轮询map,k是shardid
				_, ok := shard1[k1]
				if !ok { //新配置中有旧配置中没有,就是说之前不由我这个group负责,现在由我负责
					gid := kv.configNow.Shards[k1] //此shard在旧配置中对应的gid

					if _, ok2 := gidShard[gid]; !ok2 {
						gidShard[gid] = []int{}
					}

					gidShard[gid] = append(gidShard[gid], k1)
				}
			}

			DPrintf("shardserver:%v, after compute, gidshards:", kv.logHead)
			DPrintln(gidShard)

			var configs []shardmaster.Config
			configs = append(configs, kv.configNow)
			configs = append(configs, c)

			//groups := kv.makeGroups(kv.configs)
			groups := kv.makeGroups(configs)
			if len(gidShard) > 0 {
				shardData = kv.pullShard(groups, gidShard)
			}
		}

		//这边先处理数据,再raft,因为需要在raft日志中写入需要扩散到follower的数据
		//---此时数据都是拉到的,所以这个raft日志包含的意义是数据已经完备了
		var op Op
		op.Op = "RefreshConfig"
		op.Ts = time.Now().UnixNano() / 1000000
		op.Config = c
		//op.Table = shardData //备机怎么读是个问题啊
		op.Table = make(map[int]map[string]string)
		for k1, v1 := range shardData {
			if _, ok := op.Table[k1]; !ok {
				op.Table[k1] = make(map[string]string)
			}

			for k2, v2 := range v1 {
				op.Table[k1][k2] = v2
			}
		}

		//这里思考加锁,其实是把向raft提交日志顺序化了,我当前这种阻塞等待消息返回的模式,也只这样
		kv.mu.Lock()
		DPrintf("shardserver:%v, in queryconfig after lock\n", kv.logHead)

		var err Err
		kv.processRaft(op, &err)

		if err == OK && kv.rf.Persister.RaftStateSize() > kv.maxraftstate {
			DPrintf("shardserver:%v, RaftStateSize larggeer than the limit: %d, %d \n", kv.logHead, kv.rf.Persister.RaftStateSize(), kv.maxraftstate)
			kv.makeSnapshot(-1)
		}

		kv.mu.Unlock()

		time.Sleep(time.Duration(50 * 1000 * 1000)) //关于睡眠的必要性,在分区的情况下,你不睡的话那就完全空跑了
	}
}
