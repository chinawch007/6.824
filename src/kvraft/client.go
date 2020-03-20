package raftkv

import (
	"crypto/rand"
	"math/big"
	"time"

	"../labrpc"
)

var ClientNum int = 0

/*
	用回复rpc的方式回复clerk成功了,就是说kvserver得管理管道了是吧
	通过同一个index的日志是否出现来确定当前,任期是否变化---第二个hint说可以无限等
	server端对client端没法完全屏蔽,因为client要调多台机器
*/
//我能不能想法加个编号啊
type Clerk struct {
	servers   []*labrpc.ClientEnd
	clientNum int
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

//你这个end的初始化是怎么做的呢?为啥索引同机器号又对不上?
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientNum = ClientNum
	DPrintf("kvclient:%d init, %d server\n", ck.clientNum, len(servers))
	ClientNum++
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	var args GetArgs
	args.Key = key
	args.Ts = time.Now().UnixNano() / 1000000

	var value string

	DPrintf("kvclient:%d get start, key: %v\n", ck.clientNum, key)

	for {
		for i := 0; i < len(ck.servers); i++ {
			var reply GetReply
			ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
			if ok {
				if !reply.WrongLeader {
					if reply.Err == OK {
						DPrintf("kvclient:%d get to %d successed, key: %v, value: %v\n", ck.clientNum, i, key, reply.Value)
						value = reply.Value
						return value
					} else if reply.Err == KeyNotExist {
						DPrintf("kvclient:%d get to %d, key not exist, key: %v, value: %v\n", ck.clientNum, i, key, reply.Value)
						return ""
					} else if reply.Err == ErrRaftTimeout { //这种情况是server端的raft流程超时,并不是因为client同server间通信问题超时
						//你这样重试其实一会还得走个大循环,可能会无意义地重试某些机器
						//关键是,get如果实际执行成功有日志,我大不了多写一分,而put需要检查之前是否写过
						//究竟遇没遇到过这种错误啊
						DPrintf("kvclient:%d get to %d raft timeout, key: %v, value: %v\n", ck.clientNum, i, key, reply.Value)
					}
				} else {
					//不是leader继续找
					DPrintf("kvclient:%d get to %d wrong leader, key:%v\n", ck.clientNum, i, key)
				}
			} else {
				DPrintf("kvclient:%d get rpc to %d failed , retry, key:%v\n", ck.clientNum, i, key)
			}
		}
		time.Sleep(time.Duration(1000000000))
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//

func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	var args PutAppendArgs

	args.Key = key
	args.Value = value
	args.Op = op
	args.Retry = false
	args.Ts = time.Now().UnixNano() / 1000000

	DPrintf("kvclient:%d put start, key: %v, value:%v, op:%v\n", ck.clientNum, key, value, op)

	//---包括leader正确返回错误重试,要有限度?
	for {
		for i := 0; i < len(ck.servers); i++ {

			var reply PutAppendReply
			ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)

			if ok {
				if !reply.WrongLeader {
					if reply.Err == OK {
						DPrintf("kvclient:%d put to %d successed, key: %v, value:%v, op:%v\n", ck.clientNum, i, key, value, op)
						return
					} else if reply.Err == AlreadySuccessed {
						DPrintf("kvclient:%d put to %d already successed, key: %v, value:%v, op:%v\n", ck.clientNum, i, key, value, op)
						return
					} else if reply.Err == ErrRaftTimeout { //超时也有可能是成功的吧?
						DPrintf("kvclient:%d put to %d raft timeout, key: %v, value:%v, op:%v\n", ck.clientNum, i, key, value, op)
					}
				} else {
					DPrintf("kvclient:%d put to %d wrong leader, key: %v, value:%v, op:%v\n", ck.clientNum, i, key, value, op)
				}
			} else {
				//后来看我想到的,你随便一次查失败就重试,有可能不是针对leader的?
				//多查下倒没啥,当然你也可以直接全部请求都查...
				DPrintf("kvclient:%d put to %d rpc error, retry, key: %v, value:%v, op:%v\n", ck.clientNum, i, key, value, op)
				args.Retry = true
			}
		}
		DPrintf("kvclient:%d put after a loop not successed, need loop again,sleep awhile key: %v, value:%v, op:%v\n", ck.clientNum, key, value, op)
		time.Sleep(time.Duration(1000000000)) //关于睡眠的必要性,在分区的情况下,你不睡的话那就完全空跑了
		DPrintf("kvclient:%d put after a loop not successed, wake up,key: %v, value:%v, op:%v\n", ck.clientNum, key, value, op)
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
