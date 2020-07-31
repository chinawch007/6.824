package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK              = "OK"
	ErrNoKey        = "ErrNoKey"
	ErrRaftTimeout  = "ErrTimeout"
	AlreadyCommited = "AlreadyCommited"
	NotCommited     = "NotCommited"
	KeyNotExist     = "KeyNotExist"
	WrongGroup      = "WrongGroup"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Shard     int
	Key       string
	Value     string
	Op        string // "Put" or "Append"
	Retry     bool   //是否是因为之前的超时而重试的请求
	Ts        int64  //时间戳,用来作为请求的唯一标识
	ConfigNum int
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	//看来不需要有shard参数,能发过去就已经路由过了
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

//为了分片存储map,需要针对某个shard的map
type GetArgs struct {
	Key       string
	Ts        int64
	Shard     int
	ConfigNum int
	// You'll have to add definitions here.
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

type TransShardArgs struct {
	Gid       int
	Shards    []int
	Ts        int64
	FromGroup int
	ConfigNum int
}

type TransShardReply struct {
	WrongLeader bool
	Table       map[int]map[string]string
	ConfigNum   map[int]int
	Err         Err
}
