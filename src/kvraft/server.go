package raftkv

import (
	"bytes"
	"fmt"
	"time"

	"../raft"

	"sync"

	"../labgob"
	"../labrpc"
)

/*
	切片层按说是kvserver的功能吧,毕竟是他要读切片恢复状态
	tester会传一个界限值过来,当日志含量接近界限值时切片
	先从raft那里拿到字节序列,我执行生成一个table,在序列化成切片,这个切片,理应有我来保存---如果persister能存状态和切片的话,有raft保存也可以
	测试的时候把我搞当机用的是什么方式?

	很致命的一点,主截断之后,要让follower也截断,append->follower->pipe->kvserver---这个触发的工作交给raft而不是kv也有合理性
*/

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

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key   string
	Value string
	Op    string
	Ts    int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate  int // snapshot if log grows this big
	snapshotIndex int //
	//你用什么方式侦测到我的日志超标了,内存结构超字节,你这么厉害?

	// Your definitions here.
	Table map[string]string

	SnapshotPersister *raft.Persister
	mapCh             chan bool //当raft收到快照时,回复给server一个消息,server清理map,装载快照
}

func (kv *KVServer) isLeader() bool {
	_, isLeader := kv.rf.GetState()

	return isLeader
}

func (kv *KVServer) CommitOp(op Op) {
	if op.Op == "Put" {
		kv.Table[op.Key] = op.Value
	} else if op.Op == "Append" { //也不知道append的定义是不是这个
		kv.Table[op.Key] += op.Value
	}

	DPrintf("kvserver:%d, commit op:", kv.me)
	DPrintln(op)
}

//reply参数不能在这里
func (kv *KVServer) processRaft(op Op, err *Err) {

	//raft立即就返回了,但client那边还在等我的回复,所以我要等raft处理完,等管道
	kv.rf.Start(op)
	//DPrintf("kvserver:%d after raft start,wait for the chan msg, key:%v,value:%v\n", kv.me, args.Key, args.Value)
	for {
		//第一次通道会阻塞,但我要控制超时---这个第一次是啥意思来着?---当时为啥搞成超时模式的呢?看起来很聪明
		//这是个问题,我要搞成无限阻塞等待的还是非阻塞立即返回的???
		//time.Sleep(time.Duration(kv.WaitCommitInterval * 1000000))
		var m raft.ApplyMsg

		//此处是否阻塞等要讨论
		//---如果不等的话,一会他来了,我得到下一个请求才能接收到这次的信息,可下一次请求又会带新的数据...
		//------看能不能返回个返回码明确这种情况,下次来只是等值,不带参数---那你还不如就这么等着...
		//---一直等,我这次的请求,可能在图8的情况下丢失...
		//------得根据具体测试结果看,难...暂时先一直等吧

		//这个select流程只跑一次
		select {
		case m = <-kv.applyCh:
			{
				DPrintf("kvserver:%d, CommandValid:%t, CommandIndex:%d,", kv.me, m.CommandValid, m.CommandIndex)
				DPrintln(m.Command)

				//后来条件其实没啥用---会不会出现阻塞延迟,结果每一条消息都延迟一个阶段才来---看测试吧
				//每条都提价,但返回要看情况
				//if m.CommandValid && m.CommandIndex > kv.LastCommitIndex {
				if m.CommandValid {
					kv.CommitOp(m.Command.(Op))
				} else { //可以用来接收其他的消息
					DPrintf("kvserver:%d,exception \n", kv.me)
				}

				//前边的是都做应用了,这里一直要等到我这次提的请求才能正确返回
				//这地方是做批量提交的最后收尾
				if m.Command.(Op) == op {
					*err = OK
					return
				}
			}
		//超时有client端重试,那边会遍历所有的server
		//是说我这边给返回个超时,不是client那边等的太久自己判断出超时?
		//case <-time.After(3 * 1000 * time.Millisecond):
		case <-time.After(300 * time.Millisecond):
			{
				DPrintf("kvserver:%d,in timeout \n", kv.me)

				*err = ErrRaftTimeout
				return
			}
		}
	}

}

//我不能给客户端返回,客户端那边什么症状?
//---如果调到小部分的leader,会无限阻塞
//---所以恐怕还是要加超时机制
//---想办法辞职吧...
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("kvserver:%d, get start key:%v, ts:%d\n", kv.me, args.Key, args.Ts)
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if !kv.isLeader() {
		reply.WrongLeader = true
		DPrintf("kvserver:%d, get not leader, return, key:%v, ts:%d\n", kv.me, args.Key, args.Ts)
		return
	}
	reply.WrongLeader = false

	var op Op
	op.Op = "Get"
	op.Key = args.Key
	op.Value = "_"
	op.Ts = args.Ts

	//验证连通性,证明主依然在多数机中
	//DPrintf("kvserver:%d, before raft, key:%v, ts:%d\n", kv.me, args.Key, args.Ts)
	kv.processRaft(op, &reply.Err)
	//DPrintf("kvserver:%d, after raft, key:%v, ts:%d\n", kv.me, args.Key, args.Ts)
	if reply.Err != OK {
		return
	}

	//这样可能也并不是标准逐条的,因为我可能会一连串提交多条日志
	if reply.Err == OK && kv.rf.Persister.RaftStateSize() > kv.maxraftstate {
		DPrintf("kvserver:%d, RaftStateSize larggeer than the limit: %d, %d \n", kv.me, kv.rf.Persister.RaftStateSize(), kv.maxraftstate)
		kv.makeSnapshot(-1)
	}

	val, isExist := kv.Table[args.Key]
	if isExist {
		reply.Value = val
		reply.Err = OK
		DPrintf("kvserver:%d, get exist, key:%v, value:%v, ts:%d\n", kv.me, args.Key, val, args.Ts)
	} else {
		reply.Err = KeyNotExist
		DPrintf("kvserver:%d, get not exist, key:%v, ts:%d\n", kv.me, args.Key, args.Ts)
	}

	//如果始终不返回,那要一直阻塞,这锁也不释放吗...
	//中间穿插着不是leader又变回来也没啥,是leader就是绝对权威
	/*
		if !kv.isLeader() {
			reply.WrongLeader = true
			DPrintf("kvserver:%d get not leader after, key:%v, ts:%d\n", kv.me, args.Key, args.Ts)
		}
	*/

	DPrintf("kvserver:%d, get successed, key:%v, value:%v, ts:%d\n", kv.me, args.Key, val, args.Ts)
}

//并发调用,你就当成是多线程调用这个函数
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//DPrintf("kvserver:%d putappend start, key:%v,value:%v\n", kv.me, args.Key, args.Value)
	DPrintf("kvserver:%d, before lock, key:%v,value:%v\n", kv.me, args.Key, args.Value)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("kvserver:%d, after lock, key:%v,value:%v\n", kv.me, args.Key, args.Value)

	if !kv.isLeader() {
		reply.WrongLeader = true
		DPrintf("kvserver:%d, PutAppend not leader, return, key:%v,value:%v\n", kv.me, args.Key, args.Value)
		return
	}
	reply.WrongLeader = false

	var op Op
	op.Op = args.Op
	op.Key = args.Key
	op.Value = args.Value
	op.Ts = args.Ts //下层还是比较command,加了这个之后就能区别出其他的请求了

	//有时间戳限定,直接搜索倒是好使
	if args.Retry {
		if kv.rf.SearchLog(op) == raft.AlreadyCommited {
			reply.Err = AlreadyCommited
			return
		} else if kv.rf.SearchLog(op) == raft.NotCommited {
			//此条日志在下层的状态还没有确定,将来要么是被覆盖,要么是提交
			//这种情况下需要让client端到其他机器上继续重试
			reply.Err = NotCommited
			return
		}

	}

	DPrintf("kvserver:%d, before raft, key:%v,value:%v\n", kv.me, args.Key, args.Value)
	kv.processRaft(op, &reply.Err)
	DPrintf("kvserver:%d, after raft, key:%v,value:%v\n", kv.me, args.Key, args.Value)

	//可不可能因为raft失败造成日志不能截断而造成日志日志过度积累
	//---应该不至于,每个client最有一条未提交的日志
	if reply.Err == OK && kv.rf.Persister.RaftStateSize() > kv.maxraftstate {
		DPrintf("kvserver:%d, RaftStateSize larggeer than the limit: %d, %d \n", kv.me, kv.rf.Persister.RaftStateSize(), kv.maxraftstate)
		kv.makeSnapshot(-1)
	}

	/*
		if !kv.isLeader() {
			DPrintf("kvserver:%d PutAppend not leader after, key:%v\n", kv.me, args.Key)
			reply.WrongLeader = true
			return
		}
	*/

	DPrintf("kvserver:%d, putappend over, key:%v,value:%v, reply msg:", kv.me, args.Key, args.Value)
	DPrintln(reply)
}

//raft要保证是按序提交的,即后start的op,返回过来的值要后到
//---但还要跟leader的接口有竞争,所以加锁非阻塞模式.
//------leader是不是可以停掉这个?

func (kv *KVServer) RecvRaftMsgRoutine() {
	for {

	LOOP:
		var m raft.ApplyMsg
		select {
		case m = <-kv.applyCh:
			{
				kv.mu.Lock()
				DPrintf("kvserver:%d, follower get msg from raft CommandValid:%t, CommandIndex:%d\n", kv.me, m.CommandValid, m.CommandIndex)
				DPrintln(m)

				//后来条件其实没啥用---会不会出现阻塞延迟,结果每一条消息都延迟一个阶段才来---看测试吧
				//这个要保证一次来一条是吧
				//if m.CommandValid && m.CommandIndex == kv.LastCommitIndex+1 {
				if m.CommandValid {
					kv.CommitOp(m.Command.(Op))
					//生成快照
					kv.rf.MuLock()
					if kv.rf.Persister.RaftStateSize() > kv.maxraftstate {
						DPrintf("kvserver:%d, RaftStateSize larggeer than the limit: %d, %d \n", kv.me, kv.rf.Persister.RaftStateSize(), kv.maxraftstate)
						kv.makeSnapshot(m.CommandIndex) //暂时特殊处理下...防止过分压到了raft更新的CommitIndex
					}
					kv.rf.MuUnlock()
				} else {
					DPrintf("kvserver:%d, raft msg invalid\n", kv.me)
				}

				kv.mu.Unlock()
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

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
//这个persister是tester传进来的,所以它可以用这个来测量持久化数据大小
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.Table = make(map[string]string)

	// You may need initialization code here.

	//这边要搞成一个有缓冲的chan,这样raft那边推就不会阻塞了
	//
	//kv.applyCh = make(chan raft.ApplyMsg, 100)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.mapCh = make(chan bool)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh, kv.mapCh)

	// You may need initialization code here.

	kv.SnapshotPersister = persister
	//得加个从快照读状态的步骤
	kv.makeTableFromSnapshot() //没有快照时persister类会返回错误
	//for i := 0; i < len(kv.rf.Log); i++ {,当然那只能提交到CommitIndex,应该可以想到的
	indexLimit := kv.rf.GetMatchIndexFromLog(kv.rf.CommitIndex) //有意思,改成大写就能外部引用了
	for i := 0; i <= indexLimit; i++ {
		kv.CommitOp(kv.rf.Log[i].Command.(Op))
	}

	//专门为了raft接收到
	go kv.RecvRaftMsgRoutine()
	go kv.RecvSanpshotRoutine()

	DPrintf("kvserver:%d, start over\n", kv.me)

	return kv
}

//前半部分生成状态且从日志中删除---后来看,为啥要部分呢?直接全部搞掉呗
//快照大小限制只是限制内存大小,但我把快照搞成多大自然是我自己的事情
//我生成快照是自然地用当前的map,还是一定数目的去应用从头开始的日志?---日志在之前的明显被清了,怎么应用?
//------你之前竟然真的这么写,也是醉了
//为保证各机器的快照进度一致,要在每次应用到map之后进行检测,看是否需要压快照
//---此刻要确定,raft要清理的日志的上限是哪里?---外边传进来吧
//现在的3处调用都是外部持有锁的
func (kv *KVServer) makeSnapshot(targetIndex int) {

	//这地方不加raft的锁没啥事,大不了跟那边差几条日志---这地方要统一修正下,看看,跟所有机器的快照进度规范化有关
	//你一定要保证,这里是下层raft刚刚提交完日志才进的这个函数,保证这个index是有效的
	//index := len(kv.rf.Log) - 1
	//主调这个无论如何都是安全的
	var index int
	if targetIndex == -1 {
		index = kv.rf.GetMatchIndexFromLog(kv.rf.CommitIndex) //提交到哪压到哪,别全压
	} else {
		index = kv.rf.GetMatchIndexFromLog(targetIndex)
	}

	DPrintf("kvserver:%d, debug: %d, %d\n", kv.me, index, kv.rf.CommitIndex)
	//你这个写法其实主从的日志截断位置是不一致的

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	//都忘了快照里还有这俩东西来着
	e.Encode(kv.rf.Log[index].Index)
	e.Encode(kv.rf.Log[index].Term)
	e.Encode(kv.Table)

	data := w.Bytes()

	kv.SnapshotPersister.SaveStateAndSnapshot(nil, data)

	kv.rf.DiscardLog(index)
}

//raft是否需要加锁
func (kv *KVServer) makeTableFromSnapshot() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	data := kv.SnapshotPersister.ReadSnapshot()

	if data == nil || len(data) < 1 { // bootstrap without any state?
		DPrintf("kvserver:%d, snapshot size error\n", kv.me)
		return
	}

	//生成快照的字段,两边要对的上
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var index int
	var term int
	var table map[string]string
	//压码解码是按特定顺序的吗?---类型?
	if d.Decode(&index) != nil ||
		d.Decode(&term) != nil ||
		d.Decode(&table) != nil { //这个table直接这么搞也不知道好不好使

		DPrintf("kvserver:%d, makeTableFromSnapshot failed\n", kv.me)
	} else {
		kv.Table = table
	}
}

//raft接收到快照后
//---我要的功能是这个routine一直阻塞,直到raft发来消息
//这个问题是这样,这个触发效果一定要是非常立即的,决不能被阻塞,不然的话并行操作map会有问题
//所以这地方的危险在于,作为follower,主发来的快照和日志一起来的话,先后顺序没法保证
//所以像文档里说的,搞成一个通道吧
//这个当然只有msgtype=1时才会调用
//---如果limit设置的太小会出现刚载入完这个,接收日志的部分也生成快照
func (kv *KVServer) RecvSanpshotRoutine() {
	for {
		//会一直阻塞
		_ = <-kv.mapCh
		DPrintf("kvserver:%d, get snapshot msg from raft\n", kv.me)

		//每接到一次就装载一次吗?
		kv.makeTableFromSnapshot()
	}
}
