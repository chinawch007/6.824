package shardmaster

/*
	这里的rpc都是给管理员用的
*/

//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid---固定10个shard的话,那么group是小于这个数目的
	Groups map[int][]string // gid -> servers[]---具体机器名怎么定义得看test才知道吧
}

//他为什么要搞出这种每次更新配置都添加一个Config的模式呢?

const (
	OK              = "OK"
	ErrNoKey        = "ErrNoKey"
	ErrRaftTimeout  = "ErrTimeout"
	AlreadyCommited = "AlreadyCommited"
	NotCommited     = "NotCommited"
	KeyNotExist     = "KeyNotExist"
)

type Err string

type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings
	//可能传进来多个组
	Retry bool
	Ts    int64
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs  []int
	Retry bool
	Ts    int64
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard int
	GID   int
	Retry bool
	Ts    int64
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num int // desired config number
	Ts  int64
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

type QueryAllArgs struct {
	Ts int64
}

type QueryAllReply struct {
	WrongLeader bool
	Err         Err
	Configs     []Config
}
