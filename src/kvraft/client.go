package raftkv

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"time"

	"../labrpc"
)

/*
	多个机器同时发请求,处理好并发---看看怎么加锁---话说raft中真的不需要加什么锁吗?
	clerk发到错的机器,重试---follower机器该不该知道leader是谁?
	用回复rpc的方式回复clerk成功了,就是说kvserver得管理管道了是吧
	leader换人了,想想就头疼---需要重试
	写入raft日志的结构是op
	kvserver同raft的交流,教程中说用管道?
	start的管道,和别人的请求同时处理,并发问题,别弄出死锁了
	通过同一个index的日志是否出现来确定当前,任期是否变化---第二个hint说可以无限等
	缓存leader
	不在大多数中不该处理get请求?话说我咋知道我在不在大多数中?
	返回失败,但其实成功的情况,如何保持幂等
	超时重试,又发给另一个任期的leader

*/

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
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
//让follower告知我谁是leader还是我自己去轮询,考虑下
//因为可以无限重试,此调用一定会返回一个串
//无限循环,必保成功
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	var args GetArgs
	args.Key = key
	args.Ts = time.Now().UnixNano() / 1000000

	var value string

	fmt.Printf("kvclient get start, key: %v\n", key)
	successed := false
RELOOP:
	//一直失败,超级大循环
	for i := 0; i < len(ck.servers); i++ {
		var reply GetReply
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
		if ok {
			if !reply.WrongLeader {
				if reply.Err == OK {
					fmt.Printf("kvclient get to %d successed, key: %v, value: %v\n", i, key, reply.Value)
					value = reply.Value
					return value
				} else if reply.Err == KeyNotExist {
					fmt.Printf("kvclient get to %d, key not exist, key: %v, value: %v\n", i, key, reply.Value)
					return ""
				} else if reply.Err == ErrTimeout {
					fmt.Printf("kvclient get to %d timeout, key: %v, value: %v\n", i, key, reply.Value)
					//value = reply.Value
					//return reply.Value
					continue
				} else { //暂时没有这种错误
					//如果他是leader的话,我对他重试是没问题的
					//---这中情况下应该是当机了,我还是重新开始在来一轮吧,反正无限等待
					fmt.Printf("kvclient get to %d reply error: %v ,key:%v\n", i, reply.Err, key)
					//goto RELOOP
					break
				}
			} else {
				//不是leader继续找
				fmt.Printf("kvclient get to %d wrong leader, key:%v\n", i, key)
				continue
			}
		} else {
			//失败了要不要重试呢
			//---就是说我某一台机器调用失败了,要怎么重试?---本机重试?---重新循环?---接着跑?
			//---重新循环是合理的,这机器多半断连了
			//goto RELOOP
			//后来想接着跑挺合理

			//rpc失败要重试,以防有值但是没拿到
			fmt.Printf("kvclient get rpc to %d failed , retry, key:%v\n", i, key)
			continue
			//goto RELOOP
		}
	}

	if !successed {
		//args.Retry = true
		goto RELOOP
	}

	//循环结束没得到值,要重试
	/*
		if reply.Value == "" {
			fmt.Printf("kvclient get,after a loop not get the value, loop again, key:%v\n", key)
			goto RELOOP
		}
	*/
	//你可得保证没得到值时这个返回是空串
	//fmt.Printf("kvclient get over\n")

	return value
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
//你这个是客户端库函数,没有返回成功失败吗...
//问题在于,超时情况下,会在哪个错误条件下返回?
//无限循环,必保成功
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	var args PutAppendArgs

	args.Key = key
	args.Value = value
	args.Op = op
	args.Retry = false
	args.Ts = time.Now().UnixNano() / 1000000

	fmt.Printf("kvclient put start, key: %v, value:%v, op:%v\n", key, value, op)

	//按说我可以无限循环,必保成功,反正raft理论上会恢复
	success := false
	needRetry := false
RELOOP:

	//一直失败,超级大循环
	//---包括leader正确返回错误重试,要有限度?
	for i := 0; i < len(ck.servers); i++ {

		/*
			if mayBeChangeLeader { //要get一下,如果上次调用超时但实际set成功了,直接返回
				retGet := ck.Get(key)
				if retGet == value {
					return
				}
			}
		*/
		var reply PutAppendReply
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)

		if ok {
			if !reply.WrongLeader {
				if reply.Err == OK {
					fmt.Printf("kvclient put to %d successed, key: %v, value:%v, op:%v\n", i, key, value, op)
					success = true
					needRetry = false
					break
				} else if reply.Err == AlreadySuccessed {
					fmt.Printf("kvclient put to %d already successed, key: %v, value:%v, op:%v\n", i, key, value, op)
					success = true
					needRetry = false
					break
				} else if reply.Err == ErrTimeout {
					fmt.Printf("kvclient put to %d timeout, key: %v, value:%v, op:%v\n", i, key, value, op)
					//success = true
					//needRetry = false
					continue
				} else { //暂时没有其他错误
					//如果他是leader的话,我对他重试是没问题的
					fmt.Printf("kvclient put to %d failed, error:%v,key: %v, value:%v, op:%v\n", i, reply.Err, key, value, op)
					goto RELOOP
				}
			} else {
				fmt.Printf("kvclient put to %d wrong leader, key: %v, value:%v, op:%v\n", i, key, value, op)
				continue
			}
		} else { //这种情况下,有可能执行成功但超时了---
			//mayBeChangeLeader = true
			fmt.Printf("kvclient put to %d rpc error, retry, key: %v, value:%v, op:%v\n", i, key, value, op)
			//那我怎么检查之前成没成功啊
			//---server端想办法检查
			needRetry = true
			//args.RetryIndex =
			//goto RELOOP
			continue
		}
	}

	if needRetry {
		args.Retry = true //放在上边就错了
		goto RELOOP
	}

	//循环结束没有成功,要重试
	//---要不要睡一会,等等看
	if !success {
		fmt.Printf("kvclient put after a loop not successed, need loop again,sleep awhile key: %v, value:%v, op:%v\n", key, value, op)
		time.Sleep(time.Duration(1000000000))
		fmt.Printf("kvclient put after a loop not successed, wake up,key: %v, value:%v, op:%v\n", key, value, op)
		goto RELOOP
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
