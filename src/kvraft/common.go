package raftkv

const (
	OK              = "OK"
	ErrNoKey        = "ErrNoKey"
	ErrRaftTimeout  = "ErrTimeout"
	AlreadyCommited = "AlreadyCommited"
	NotCommited     = "NotCommited"
	KeyNotExist     = "KeyNotExist"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key        string
	Value      string
	Op         string // "Put" or "Append"
	Retry      bool   //是否是因为之前的超时而重试的请求
	RetryIndex int    //这次是重试,上次的写应该写到哪个位置
	Ts         int64  //时间戳,用来作为请求的唯一标识
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	Ts  int64
	// You'll have to add definitions here.
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}
