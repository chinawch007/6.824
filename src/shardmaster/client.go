package shardmaster

//
// Shardmaster clerk.
//

import "../labrpc"
import "time"
import "crypto/rand"
import "math/big"

var ClientNum int = 0

type Clerk struct {
	servers   []*labrpc.ClientEnd
	clientNum int
	// Your data here.
	logHead string
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd, logHead string) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientNum = ClientNum
	DPrintf(ck.logHead, "kvclient:%d init, %d server\n", ck.clientNum, len(servers))
	ClientNum++
	ck.logHead = logHead
	// Your code here.
	return ck
}

//直接用裸参数,不用args类,对外透明
func (ck *Clerk) Query(num int) Config {
	/*
		args := &QueryArgs{}
		// Your code here.
		args.Num = num
		for {
			// try each known server.
			for _, srv := range ck.servers {
				var reply QueryReply
				ok := srv.Call("ShardMaster.Query", args, &reply)
				if ok && reply.WrongLeader == false {
					return reply.Config
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
	*/

	var args QueryArgs
	args.Num = num
	args.Ts = time.Now().UnixNano() / 1000000

	var c Config

	DPrintf(ck.logHead, "masterclient:%d query start, num: %d\n", ck.clientNum, num)

	for {
		for i := 0; i < len(ck.servers); i++ {
			var reply QueryReply
			ok := ck.servers[i].Call("ShardMaster.Query", &args, &reply)
			if ok {
				if !reply.WrongLeader {
					if reply.Err == OK {
						c = reply.Config
						DPrintf(ck.logHead, "masterclient:%d query to %d successed, num: %d", ck.clientNum, i, num)
						DPrintln(c)
						return c
					} else if reply.Err == KeyNotExist {
						DPrintf(ck.logHead, "masterclient:%d query to %d, key not exist, num: %d\n", ck.clientNum, i, num)
						return c //这个c是完全空的
					} else if reply.Err == ErrRaftTimeout { //这种情况是server端的raft流程超时,并不是因为client同server间通信问题超时
						//你这样重试其实一会还得走个大循环,可能会无意义地重试某些机器
						//关键是,get如果实际执行成功有日志,我大不了多写一分,而put需要检查之前是否写过
						//究竟遇没遇到过这种错误啊
						DPrintf(ck.logHead, "masterclient:%d query to %d raft timeout, num: %d\n", ck.clientNum, i, num)
					}
				} else {
					//不是leader继续找
					DPrintf(ck.logHead, "masterclient:%d query to %d wrong leader, num:%d\n", ck.clientNum, i, num)
				}
			} else {
				DPrintf(ck.logHead, "masterclient:%d query rpc to %d failed , retry, num:%d\n", ck.clientNum, i, num)
			}
		}
		time.Sleep(time.Duration(1000 * 1000 * 1000))
	}
}

func (ck *Clerk) QueryAll() []Config {
	var args QueryAllArgs
	args.Ts = time.Now().UnixNano() / 1000000

	var cs []Config

	DPrintf(ck.logHead, "masterclient:%d queryall start\n", ck.clientNum)

	for {
		for i := 0; i < len(ck.servers); i++ {
			var reply QueryAllReply
			ok := ck.servers[i].Call("ShardMaster.QueryAll", &args, &reply)
			if ok {
				if !reply.WrongLeader {
					if reply.Err == OK {
						cs = reply.Configs
						DPrintf(ck.logHead, "masterclient:%d queryall to %d successed", ck.clientNum, i)
						DPrintln(cs)
						return cs
					} else if reply.Err == KeyNotExist {
						DPrintf(ck.logHead, "masterclient:%d queryall to %d, key not exist\n", ck.clientNum, i)
						return cs //这个c是完全空的
					} else if reply.Err == ErrRaftTimeout { //这种情况是server端的raft流程超时,并不是因为client同server间通信问题超时
						//你这样重试其实一会还得走个大循环,可能会无意义地重试某些机器
						//关键是,get如果实际执行成功有日志,我大不了多写一分,而put需要检查之前是否写过
						//究竟遇没遇到过这种错误啊
						DPrintf(ck.logHead, "masterclient:%d queryall to %d raft timeout\n", ck.clientNum, i)
					}
				} else {
					//不是leader继续找
					DPrintf(ck.logHead, "masterclient:%d query to %d wrong leader\n", ck.clientNum, i)
				}
			} else {
				DPrintf(ck.logHead, "masterclient:%d queryall rpc to %d failed , retry\n", ck.clientNum, i)
			}
		}
		time.Sleep(time.Duration(1000 * 1000 * 1000))
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	/*
		args := &JoinArgs{}
		// Your code here.
		args.Servers = servers

		for {
			// try each known server.
			for _, srv := range ck.servers {
				var reply JoinReply
				ok := srv.Call("ShardMaster.Join", args, &reply)
				if ok && reply.WrongLeader == false {
					return
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
	*/

	var args JoinArgs

	args.Servers = servers
	args.Retry = false
	args.Ts = time.Now().UnixNano() / 1000000

	DPrintf(ck.logHead, "masterclient:%d join start:", ck.clientNum)
	DPrintln(args)

	j := -1
	for {
		j++
		for i := 0; i < len(ck.servers); i++ {

			var reply JoinReply
			ok := ck.servers[i].Call("ShardMaster.Join", &args, &reply)

			if ok {
				if !reply.WrongLeader {
					if reply.Err == OK {
						DPrintf(ck.logHead, "masterclient:%d join to %d successed", ck.clientNum, i)
						DPrintln(args)
						return
					} else if reply.Err == AlreadyCommited {
						DPrintf(ck.logHead, "masterclient:%d join to %d already successed", ck.clientNum, i)
						DPrintln(args)
						return
					} else if reply.Err == ErrRaftTimeout { //超时也有可能是成功的吧?
						DPrintf(ck.logHead, "masterclient:%d join to %d raft timeout\n", ck.clientNum, i)
						args.Retry = true //这地方要写上啊,不仅是rpc失败的时候
					} else if reply.Err == NotCommited { //继续循环,到其他机器上重试
						DPrintf(ck.logHead, "masterclient:%d join to %d log not commited\n", ck.clientNum, i)
						if j >= 3 { //可能会有下一轮,我每轮提一次
							ck.PutEmptyLog(i)
						}
					}
				} else {
					DPrintf(ck.logHead, "masterclient:%d join to %d wrong leader\n", ck.clientNum, i)
				}
			} else {
				//后来看我想到的,你随便一次查失败就重试,有可能不是针对leader的?
				//多查下倒没啥,当然你也可以直接全部请求都查...
				DPrintf(ck.logHead, "masterclient:%d join to %d rpc error, retry\n", ck.clientNum, i)
				args.Retry = true
			}
		}
		DPrintf(ck.logHead, "masterclient:%d join after a loop not successed, need loop again,sleep awhile\n", ck.clientNum)
		time.Sleep(time.Duration(1000 * 1000 * 1000)) //关于睡眠的必要性,在分区的情况下,你不睡的话那就完全空跑了
		DPrintf(ck.logHead, "masterclient:%d join after a loop not successed, wake up\n", ck.clientNum)
	}
}

func (ck *Clerk) Leave(gids []int) {
	/*
		args := &LeaveArgs{}
		// Your code here.
		args.GIDs = gids

		for {
			// try each known server.
			for _, srv := range ck.servers {
				var reply LeaveReply
				ok := srv.Call("ShardMaster.Leave", args, &reply)
				if ok && reply.WrongLeader == false {
					return
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
	*/

	var args LeaveArgs

	args.GIDs = gids
	args.Retry = false
	args.Ts = time.Now().UnixNano() / 1000000

	DPrintf(ck.logHead, "masterclient:%d leave start:", ck.clientNum)
	DPrintln(args)

	j := -1
	for {
		j++
		for i := 0; i < len(ck.servers); i++ {

			var reply LeaveReply
			ok := ck.servers[i].Call("ShardMaster.Leave", &args, &reply)

			if ok {
				if !reply.WrongLeader {
					if reply.Err == OK {
						DPrintf(ck.logHead, "masterclient:%d leave to %d successed", ck.clientNum, i)
						DPrintln(args)
						return
					} else if reply.Err == AlreadyCommited {
						DPrintf(ck.logHead, "masterclient:%d leave to %d already successed", ck.clientNum, i)
						DPrintln(args)
						return
					} else if reply.Err == ErrRaftTimeout { //超时也有可能是成功的吧?
						DPrintf(ck.logHead, "masterclient:%d leave to %d raft timeout\n", ck.clientNum, i)
						args.Retry = true //这地方要写上啊,不仅是rpc失败的时候
					} else if reply.Err == NotCommited { //继续循环,到其他机器上重试
						DPrintf(ck.logHead, "masterclient:%d leave to %d log not commited\n", ck.clientNum, i)
						if j >= 3 { //可能会有下一轮,我每轮提一次
							ck.PutEmptyLog(i)
						}
					}
				} else {
					DPrintf(ck.logHead, "masterclient:%d leave to %d wrong leader\n", ck.clientNum, i)
				}
			} else {
				//后来看我想到的,你随便一次查失败就重试,有可能不是针对leader的?
				//多查下倒没啥,当然你也可以直接全部请求都查...
				DPrintf(ck.logHead, "masterclient:%d leave to %d rpc error, retry\n", ck.clientNum, i)
				args.Retry = true
			}
		}
		DPrintf(ck.logHead, "masterclient:%d leave after a loop not successed, need loop again,sleep awhile\n", ck.clientNum)
		time.Sleep(time.Duration(1000 * 1000 * 1000)) //关于睡眠的必要性,在分区的情况下,你不睡的话那就完全空跑了
		DPrintf(ck.logHead, "masterclient:%d leave after a loop not successed, wake up\n", ck.clientNum)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	/*
		args := &MoveArgs{}
		// Your code here.
		args.Shard = shard
		args.GID = gid

		for {
			// try each known server.
			for _, srv := range ck.servers {
				var reply MoveReply
				ok := srv.Call("ShardMaster.Move", args, &reply)
				if ok && reply.WrongLeader == false {
					return
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
	*/

	var args MoveArgs

	args.Shard = shard
	args.GID = gid
	args.Retry = false
	args.Ts = time.Now().UnixNano() / 1000000

	DPrintf(ck.logHead, "masterclient:%d move start:", ck.clientNum)
	DPrintln(args)

	j := -1
	for {
		j++
		for i := 0; i < len(ck.servers); i++ {

			var reply MoveReply
			ok := ck.servers[i].Call("ShardMaster.Move", &args, &reply)

			if ok {
				if !reply.WrongLeader {
					if reply.Err == OK {
						DPrintf(ck.logHead, "masterclient:%d move to %d successed", ck.clientNum, i)
						DPrintln(args)
						return
					} else if reply.Err == AlreadyCommited {
						DPrintf(ck.logHead, "masterclient:%d move to %d already successed", ck.clientNum, i)
						DPrintln(args)
						return
					} else if reply.Err == ErrRaftTimeout { //超时也有可能是成功的吧?
						DPrintf(ck.logHead, "masterclient:%d move to %d raft timeout\n", ck.clientNum, i)
						args.Retry = true //这地方要写上啊,不仅是rpc失败的时候
					} else if reply.Err == NotCommited { //继续循环,到其他机器上重试
						DPrintf(ck.logHead, "masterclient:%d move to %d log not commited\n", ck.clientNum, i)
						if j >= 3 { //可能会有下一轮,我每轮提一次
							ck.PutEmptyLog(i)
						}
					}
				} else {
					DPrintf(ck.logHead, "masterclient:%d move to %d wrong leader\n", ck.clientNum, i)
				}
			} else {
				//后来看我想到的,你随便一次查失败就重试,有可能不是针对leader的?
				//多查下倒没啥,当然你也可以直接全部请求都查...
				DPrintf(ck.logHead, "masterclient:%d move to %d rpc error, retry\n", ck.clientNum, i)
				args.Retry = true
			}
		}
		DPrintf(ck.logHead, "masterclient:%d move after a loop not successed, need loop again,sleep awhile\n", ck.clientNum)
		time.Sleep(time.Duration(1000 * 1000 * 1000)) //关于睡眠的必要性,在分区的情况下,你不睡的话那就完全空跑了
		DPrintf(ck.logHead, "masterclient:%d move after a loop not successed, wake up\n", ck.clientNum)
	}
}

func (ck *Clerk) PutEmptyLog(i int) {
	// You will have to modify this function.
	var args LeaveArgs

	args.GIDs = []int{}
	args.Retry = false
	args.Ts = time.Now().UnixNano() / 1000000

	DPrintf(ck.logHead, "masterclient:%d emptylog start\n", ck.clientNum)

	var reply LeaveReply
	ok := ck.servers[i].Call("ShardMaster.Leave", &args, &reply)

	if ok {
		if !reply.WrongLeader {
			if reply.Err == OK {
				DPrintf(ck.logHead, "masterclient:%d emptylog to %d successed\n", ck.clientNum, i)
				return
			} else if reply.Err == ErrRaftTimeout { //超时也有可能是成功的吧?
				DPrintf(ck.logHead, "masterclient:%d emptylog to %d raft timeout\n", ck.clientNum, i)
				args.Retry = true //这里这个参数没啥用了,我只提一次
			}
		} else {
			DPrintf(ck.logHead, "masterclient:%d emptylog to %d wrong leader\n", ck.clientNum, i)
		}
	} else {
		DPrintf(ck.logHead, "masterclient:%d emptylog to %d rpc error, retry\n", ck.clientNum, i)
		args.Retry = true
	}

	DPrintf(ck.logHead, "masterclient:%d emptylog fail\n", ck.clientNum)
}
