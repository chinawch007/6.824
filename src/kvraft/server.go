package raftkv

import (
	"bytes"
	"fmt"
	"log"
	"reflect"
	"time"

	"../raft"

	"sync"

	"../labgob"
	"../labrpc"
)

/*
	切片层按说是kvserver的功能吧,毕竟是他要读切片恢复状态
	不能再有对要回收的内存的引用,go本身的垃圾回收
	tester会传一个界限值过来,当日志含量接近界限值时切片
	persister.SaveStateAndSnapshot()---这东西该是谁负责呢?kvserver倒也可以用这个类
	raft主发起切片给follower,然后再传给kvserver?---有点理解了,raft当然只是日志的管理员,解释全在kvserver手里,切片应该是一个map序列化后的字节序列
	先从raft那里拿到字节序列,我执行生成一个table,在序列化成切片,这个切片,理应有我来保存---如果persister能存状态和切片的话,有raft保存也可以
	测试的时候把我搞当机用的是什么方式?

	怎么抛弃旧日志是个问题,旧日志不能再有引用,用go的垃圾回收?
	快照除了存成map还有其他方式吗?
	很致命的一点,主截断之后,要让follower也截断,append->follower->pipe->kvserver---这个触发的工作交给raft而不是kv也有合理性
	---我为什么要在rpc中传快照信息,话说我就让follower自己截断日志不行吗?
*/

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
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

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	Table map[string]string

	LastCommitIndex int
	//LastCommitTerm  int

	WaitCommitInterval int

	//看有没有必要就复用raft的那个persister---牛马不相及复用啥
	SnapshotPersister *raft.Persister
}

func (kv *KVServer) isLeader() bool {
	_, isLeader := kv.rf.GetState()

	return isLeader
}

//我不能给客户端返回,客户端那边什么症状?
//---如果调到小部分的leader,会无限阻塞
//---所以恐怕还是要加超时机制
//---想办法辞职吧...
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//这个锁跟raft的锁没关系,你加了raft也可能不是leader了,只能get完之后检查下
	fmt.Printf("kvserver:%d get start key:%v\n", kv.me, args.Key)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if !kv.isLeader() {
		reply.WrongLeader = true
		fmt.Printf("kvserver:%d get not leader, return, key:%v\n", kv.me, args.Key)
		return
	}

	reply.WrongLeader = false
	val, isExist := kv.Table[args.Key]
	if isExist {
		reply.Value = val
		reply.Err = OK
		fmt.Printf("kvserver:%d get successed, key:%v, value:%v\n", kv.me, args.Key, val)
	} else {
		reply.Err = KeyNotExist
		fmt.Printf("kvserver:%d get not exist, key:%v\n", kv.me, args.Key)
		//不能返回,你找不到新主或许能找到
	}

	//此部分从putAppend搬来,验证连通性,证明主依然在多数机中
	var op Op
	op.Op = "Get"
	op.Key = args.Key
	op.Value = "_"

	kv.rf.Start(op)
	//fmt.Printf("kvserver:%d after raft start,wait for the chan msg, key:%v,value:%v\n", kv.me, args.Key, args.Value)

	for {
		var m raft.ApplyMsg
		select {
		case m = <-kv.applyCh:
			{
				fmt.Printf("kvserver:%d get the msg from chan, key:%v\n", kv.me, args.Key)
				fmt.Printf("kvserver:%d CommandValid:%t, CommandIndex:%d,", kv.me, m.CommandValid, m.CommandIndex)
				fmt.Println(m.Command)
				/*
					if m.Command.(Op) != op {
						kv.CommitOp(m.Command.(Op)) //暂时先应用下
						continue
					}
				*/

				if m.CommandValid && m.CommandIndex > kv.LastCommitIndex {
					kv.CommitOp(m.Command.(Op))
					kv.LastCommitIndex = m.CommandIndex
					reply.Err = OK
					reply.WrongLeader = false
					//return
				} else {
					//超时了,实际可能是提交了的,然后我这边怎么处理呢?---get一下,看看是不是设定的值
					//reply.Err = ErrTimeout
					//reply.WrongLeader = false
					fmt.Printf("kvserver:%d,exception \n", kv.me)
				}

				if m.Command.(Op) == op {
					fmt.Printf("kvserver:%d reply msg:", kv.me)
					fmt.Println(reply)
					return
				}
			}
		case <-time.After(30 * 100 * time.Millisecond):
			{
				reply.Err = ErrTimeout
				return
			}
		default:
		}
	}
	//接收后不做行动了,只是为了验证多数连通性
	//如果始终不返回,那要一直阻塞,这锁也不释放吗...
	//中间穿插着不是leader又变回来也没啥,是leader就是绝对权威
	if !kv.isLeader() {
		reply.WrongLeader = true
		fmt.Printf("kvserver:%d get not leader after, key:%v\n", kv.me, args.Key)
	}
}

func (kv *KVServer) CommitOp(op Op) {
	if op.Op == "Put" {
		kv.Table[op.Key] = op.Value
	} else if op.Op == "Append" { //也不知道append的定义是不是这个
		kv.Table[op.Key] += op.Value
	}

	fmt.Printf("kvserver:%d commit value into table:%v\n", kv.me, kv.Table[op.Key])
}

//raft要保证是按序提交的,即后start的op,返回过来的值要后到---这是他们讨论过的乱序提交吗?
//看看这个routine是持久存在的好还是每个请求一个的好---一个就够了,不然多个routine还需要多个chan
//---follower的msg如果不读出来有积存,raft就不能往里推了,所以必然得有这么个routine平时去读,并且把日志应用于Table
//---但还要跟leader的接口有竞争,所以加锁非阻塞模式.

//这个基本上只为了follower服务了,leader是不会接收消息引起这个的
func (kv *KVServer) RecvRaftMsgRoutine() {
	for {
		kv.mu.Lock()
		//fmt.Printf("kvserver:%d lock\n", kv.me)
		var m raft.ApplyMsg

		select {
		case m = <-kv.applyCh:
			{
				fmt.Printf("kvserver:%d follower get msg from raft CommandValid:%t, CommandIndex:%d\n", kv.me, m.CommandValid, m.CommandIndex)
				fmt.Println(m.Command)

				//对get命令做特殊处理
				/*
					if m.Command.(Op).Op == "Get" {
						fmt.Printf("kvserver:%d, Get command msg, ignored\n", kv.me)
						continue
					}
				*/

				//后来条件其实没啥用---会不会出现阻塞延迟,结果每一条消息都延迟一个阶段才来---看测试吧
				//这个要保证一次来一条是吧
				if m.CommandValid && m.CommandIndex == kv.LastCommitIndex+1 {
					kv.CommitOp(m.Command.(Op))
					kv.LastCommitIndex++
				} else {
					//超时了,实际可能是提交了的,然后我这边怎么处理呢?---get一下,看看是不是设定的值
					fmt.Printf("kvserver:%d follower not get msg from raft\n", kv.me)
				}
			}
		default:
			{

			}

		}

		kv.mu.Unlock()
		//fmt.Printf("kvserver:%d unlock\n", kv.me)
		time.Sleep(time.Duration(200 * 1000000))
	}
}

//并发调用,你就当成是多线程调用这个函数
//---发现了个问题,新主提交旧日志的时候,我这里接收到的不是目的值---描述下我这种1个1个提交的运行状态
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	fmt.Printf("kvserver:%d putappend start, key:%v,value:%v\n", kv.me, args.Key, args.Value)
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if !kv.isLeader() {
		reply.WrongLeader = true
		fmt.Printf("kvserver:%d PutAppend not leader, return, key:%v\n", kv.me, args.Key)
		return
	}

	reply.TargetIndex = len(kv.rf.Log)

	var op Op
	op.Op = args.Op
	op.Key = args.Key
	op.Value = args.Value
	op.Ts = args.Ts //下层还是比较command,加了这个之后就能区别出其他的请求了

	if args.Retry && kv.rf.SearchLog(op) {
		//if args.Retry && kv.rf.Log[args.RetryIndex].Command.(Op) == op {
		reply.Err = AlreadySuccessed
		return
	}

	//并发被调,等待raft管道返回,返回的顺序不确定,不知道应答对应哪个请求,加锁顺序调用吧---raft那边其实也没啥并发度吧
	//---其实根据管道过来的消息加个字段可以知道具体的应答client
	//---你说的这样得你自己做事件驱动消息分发,这里边的框架就是一个函数完成后要结果

	//raft立即就返回了,但client那边还在等我的回复,所以我要等raft处理完,等管道
	kv.rf.Start(op)
	fmt.Printf("kvserver:%d after raft start,wait for the chan msg, key:%v,value:%v\n", kv.me, args.Key, args.Value)

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

		select {
		case m = <-kv.applyCh:
			{
				fmt.Printf("kvserver:%d get the msg from chan, key:%v,value:%v\n", kv.me, args.Key, args.Value)
				fmt.Printf("kvserver:%d CommandValid:%t, CommandIndex:%d,", kv.me, m.CommandValid, m.CommandIndex)
				fmt.Println(m.Command)

				//第一次接收可能会接收多条,我要一直等直到收到这次这条
				/*
					if m.Command.(Op) != op {
						kv.CommitOp(m.Command.(Op)) //暂时先应用下
						continue
					}
				*/

				//后来条件其实没啥用---会不会出现阻塞延迟,结果每一条消息都延迟一个阶段才来---看测试吧
				//每条都提价,但返回要看情况
				if m.CommandValid && m.CommandIndex > kv.LastCommitIndex {
					kv.CommitOp(m.Command.(Op))
					kv.LastCommitIndex = m.CommandIndex
					reply.Err = OK
					reply.WrongLeader = false
					//return
				} else {
					//超时了,实际可能是提交了的,然后我这边怎么处理呢?---get一下,看看是不是设定的值
					//reply.Err = ErrTimeout
					//reply.WrongLeader = false
					fmt.Printf("kvserver:%d,exception \n", kv.me)
				}

				//前边的是都做应用了,这里一直要等到我这次提的请求才能正确返回
				if m.Command.(Op) == op {
					fmt.Printf("kvserver:%d reply msg:", kv.me)
					fmt.Println(reply)
					return
				}
			}
		//超时有client端重试,那边会遍历所有的server
		case <-time.After(3 * 1000 * time.Millisecond):
			{
				reply.Err = ErrTimeout
				reply.WrongLeader = false
				return
			}
		default:
		}

	}

	/*
		if kv.rf.persister.RaftStateSize() > kv.maxraftstate {
			kv.makeSnapshot()
		}
	*/

	if !kv.isLeader() {
		fmt.Printf("kvserver:%d PutAppend not leader after, key:%v\n", kv.me, args.Key)
		reply.WrongLeader = true
		return
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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.Table = make(map[string]string)

	// You may need initialization code here.

	//这边要搞成一个有缓冲的chan,这样raft那边推就不会阻塞了,
	kv.applyCh = make(chan raft.ApplyMsg, 100)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.LastCommitIndex = 0
	//kv.LastCommitTerm = -1

	kv.SnapshotPersister = new(raft.Persister)

	for i := 0; i < len(kv.rf.Log); i++ {
		kv.CommitOp(kv.rf.Log[i].Command.(Op))
	}

	go kv.RecvRaftMsgRoutine()

	fmt.Printf("kvserver:%dstart over\n", kv.me)

	return kv
}

//前半部分生成状态且从日志中删除---后来看,为啥要部分呢?直接全部搞掉呗
func (kv *KVServer) makeSnapshot() {

	//这地方不加raft的锁没啥事,大不了跟那边差几条日志
	index := len(kv.rf.Log) - 1

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.rf.Log[index].Index)
	e.Encode(kv.rf.Log[index].Term)

	var table map[string]string
	for i := 0; i <= index; i++ {
		reflect.TypeOf(kv.rf.Log[i].Command)
		op := kv.rf.Log[i].Command.(Op)

		if op.Op == "Put" {
			table[op.Key] = table[op.Value]
		} else if op.Op == "Append" {
			table[op.Key] += table[op.Value]
		}
	}
	e.Encode(table)

	data := w.Bytes()

	kv.SnapshotPersister.SaveStateAndSnapshot(nil, data)

	kv.rf.DiscardLog(index)
}
