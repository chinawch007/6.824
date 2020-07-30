package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardmaster to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

/*
	轮询kvserver找主啊
	调用过程中主换了,我这边要处理,可以无限等待
*/

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"strconv"
	"time"

	"../labrpc"
	"../shardmaster"
)

var ClientNum int = 0

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	fmt.Printf("key2shard key0:%c, shard:%d\n", key[0], shard)
	shard %= shardmaster.NShards
	fmt.Printf("key2shard key:%v, shard:%d\n", key, shard)
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

//包之间的包含关系上,shard是包含master的
type Clerk struct {
	sm       *shardmaster.Clerk
	config   shardmaster.Config
	make_end func(string) *labrpc.ClientEnd //不像kvraft有直接有其他机器的代号
	// You will have to modify this struct.
	clientNum int
}

//
// the tester calls MakeClerk.
//
// masters[] is needed to call shardmaster.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
//config成员怎么初始化呢?
func MakeClerk(masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	var logHead string
	ck := new(Clerk)
	ck.clientNum = ClientNum
	logHead = "shardclient" + strconv.Itoa(ck.clientNum)
	ck.sm = shardmaster.MakeClerk(masters, logHead)
	ck.make_end = make_end
	ClientNum++
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.Shard = key2shard(key)
	args.Key = key
	args.Ts = time.Now().UnixNano() / 1000000

	DPrintf("shardclient:%d get start, key: %v\n", ck.clientNum, key)

	for {
	NewConfig:
		ck.config = ck.sm.Query(-1)
		args.ConfigNum = ck.config.Num

		//shard到group还有层转换
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok { //这个ok是map返回的?
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok {
					if !reply.WrongLeader {
						if reply.Err == OK {
							DPrintf("shardclient:%d get to %v successed, key: %v, value: %v\n", ck.clientNum, servers[si], key, reply.Value)
							return reply.Value
						} else if reply.Err == WrongGroup {
							DPrintf("shardclient:%d get to %v, wronggroup, key: %v, value: %v\n", ck.clientNum, servers[si], key, reply.Value)
						} else if reply.Err == KeyNotExist {
							DPrintf("shardclient:%d get to %v, key not exist, key: %v, value: %v\n", ck.clientNum, servers[si], key, reply.Value)
							return ""
						} else if reply.Err == ErrRaftTimeout { //这种情况是server端的raft流程超时,并不是因为client同server间通信问题超时
							//你这样重试其实一会还得走个大循环,可能会无意义地重试某些机器
							//关键是,get如果实际执行成功有日志,我大不了多写一分,而put需要检查之前是否写过
							//究竟遇没遇到过这种错误啊
							DPrintf("shardclient:%d get to %v raft timeout, key: %v, value: %v\n", ck.clientNum, servers[si], key, reply.Value)
						} else if reply.Err == "ConfigNumNotMatch" {
							DPrintf("shardclient:%d get to %v confignum not match, key: %v, value: %v\n", ck.clientNum, servers[si], key, reply.Value)
							goto NewConfig
						} else if reply.Err == "ShardNotReady" {
							DPrintf("shardclient:%d get to %v shard not ready, key: %v, value: %v\n", ck.clientNum, servers[si], key, reply.Value)
							//其实下次还是应该读它
							si--
						}
					} else {
						//不是leader继续找
						DPrintf("shardclient:%d get to %v wrong leader, key:%v\n", ck.clientNum, servers[si], key)
					}
				} else {
					DPrintf("shardclient:%d get rpc to %v failed , retry, key:%v\n", ck.clientNum, servers[si], key)
				}

			}
		}
		//time.Sleep(100 * time.Millisecond)
		time.Sleep(time.Duration(1000000000))
	}

	//return ""
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{}
	args.Shard = key2shard(key)
	args.Key = key
	args.Value = value
	args.Op = op
	args.Retry = false
	//args.Ts = time.Now().UnixNano() / 1000000
	args.Ts = time.Now().UnixNano()
	DPrintf("shardclient:%d start, key: %v, value:%v, op:%v, ts:%d\n", ck.clientNum, key, value, op, args.Ts)

	//这里要注意下,只有在group是正确的情况下,重试才是有意义的
	j := -1
	for {
		//每轮都查一回,频率算比较高的了
		//---这样查询master都会写日志,并发有点多
	NewConfig:
		ck.config = ck.sm.Query(-1)
		args.ConfigNum = ck.config.Num

		DPrintf("shardclient:%d use config num:%d\n", ck.clientNum, args.ConfigNum)

		j++
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok {
					if !reply.WrongLeader {
						if reply.Err == OK {
							DPrintf("shardclient:%d put to %v successed, key: %v, value:%v, op:%v\n", ck.clientNum, servers[si], key, value, op)
							return
						} else if reply.Err == WrongGroup {
							DPrintf("shardclient:%d put to %v, wronggroup, key: %v, value: %v\n", ck.clientNum, servers[si], key, value)
							goto NewConfig //后加的,不想看那么多废日志
							//这其实就白嫖了一下,continue一会儿再拉下配置
						} else if reply.Err == AlreadyCommited {
							DPrintf("shardclient:%d put to %v already successed, key: %v, value:%v, op:%v\n", ck.clientNum, servers[si], key, value, op)
							return
						} else if reply.Err == ErrRaftTimeout { //超时也有可能是成功的吧?
							DPrintf("shardclient:%d put to %v raft timeout, key: %v, value:%v, op:%v\n", ck.clientNum, servers[si], key, value, op)
							args.Retry = true //这地方要写上啊,不仅是rpc失败的时候
							si--
						} else if reply.Err == NotCommited { //继续循环,到其他机器上重试
							DPrintf("shardclient:%d put to %v log not commited, key: %v, value:%v, op:%v\n", ck.clientNum, servers[si], key, value, op)
							if j >= 3 { //可能会有下一轮,我每轮提一次
								ck.PutEmptyLog(servers[si])
							}
						} else if reply.Err == "ConfigNumNotMatch" {
							DPrintf("shardclient:%d put to %v confignum not match, key: %v, value: %v\n", ck.clientNum, servers[si], key, value)
							goto NewConfig
						} else if reply.Err == "ShardNotReady" {
							DPrintf("shardclient:%d get to %v shard not ready, key: %v, value: %v\n", ck.clientNum, servers[si], key, value)
							//其实下次还是应该读它
							si--
						}
					} else {
						DPrintf("shardclient:%d put to %v wrong leader, key: %v, value:%v, op:%v\n", ck.clientNum, servers[si], key, value, op)
					}
				} else {
					//后来看我想到的,你随便一次查失败就重试,有可能不是针对leader的?
					//多查下倒没啥,当然你也可以直接全部请求都查...
					DPrintf("shardclient:%d put to %v rpc error, retry, key: %v, value:%v, op:%v\n", ck.clientNum, servers[si], key, value, op)
					args.Retry = true
					si-- //试下
				}
			}
			DPrintf("shardclient:%d put after a loop not successed, need loop again,sleep awhile key: %v, value:%v, op:%v\n", ck.clientNum, key, value, op)
			time.Sleep(time.Duration(1000000000)) //关于睡眠的必要性,在分区的情况下,你不睡的话那就完全空跑了
			DPrintf("shardclient:%d put after a loop not successed, wake up,key: %v, value:%v, op:%v\n", ck.clientNum, key, value, op)
		}
	}
}

//要把shard写进来,server端可能会拒绝
func (ck *Clerk) PutEmptyLog(serverName string) {
	// You will have to modify this function.
	var args PutAppendArgs

	args.Key = "wang"
	args.Value = "chong"
	args.Op = "Put"
	args.Retry = false
	args.Ts = time.Now().UnixNano() / 1000000

	DPrintf("shardclient:%d put start, key: %v, value:%v, op:%v\n", ck.clientNum, args.Key, args.Value, args.Op)

	var reply PutAppendReply
	srv := ck.make_end(serverName)
	ok := srv.Call("KVServer.PutAppend", &args, &reply)
	if ok {
		if !reply.WrongLeader {
			if reply.Err == OK {
				DPrintf("shardclient:%d put to %v successed, key: %v, value:%v, op:%v\n", ck.clientNum, serverName, args.Key, args.Value, args.Op)
				return
			} else if reply.Err == ErrRaftTimeout { //超时也有可能是成功的吧?
				DPrintf("shardclient:%d put to %v raft timeout, key: %v, value:%v, op:%v\n", ck.clientNum, serverName, args.Key, args.Value, args.Op)
				args.Retry = true //这里这个参数没啥用了,我只提一次
			}
		} else {
			DPrintf("shardclient:%d put to %v wrong leader, key: %v, value:%v, op:%v\n", ck.clientNum, serverName, args.Key, args.Value, args.Op)
		}
	} else {
		DPrintf("shardclient:%d put to %v rpc error, retry, key: %v, value:%v, op:%v\n", ck.clientNum, serverName, args.Key, args.Value, args.Op)
		args.Retry = true
	}

	DPrintf("shardclient:%d empty put fail, key: %v, value:%v, op:%v\n", ck.clientNum, args.Key, args.Value, args.Op)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

/*
func (ck *Clerk) pullConfig() {
	DPrintf("shardclient:%d pullconfig start\n", kv.me)

	var args shardmaster.QueryArgs

	args.Num = -1
	args.Ts = time.Now().UnixNano() / 1000000

	for {
		for i := 0; i < len(kv.masters); i++ {
			var reply shardmaster.QueryReply
			ok := kv.masters[i].Call("ShardMaster.Query", &args, &reply)
			if ok {
				if !reply.WrongLeader {
					if reply.Err == OK {
						DPrintf("shardclient:%d pullconfig to %d successed\n", ck.clientNum, i)
						return
					} else if reply.Err == ErrRaftTimeout {
						DPrintf("shardclient:%d pullconfig to %d raft timeout\n", ck.clientNum, i)
					}
				} else {
					DPrintf("shardclient:%d pullconfig to %d wrong leader\n", ck.clientNum, i)
				}
			} else {
				DPrintf("shardclient:%d pullconfig rpc to %d failed , retry\n", ck.clientNum, i)
			}
		}
		time.Sleep(time.Duration(1000 * 1000 * 1000))
	}
}
*/
