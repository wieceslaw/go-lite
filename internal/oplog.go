package internal

import "Golite/internal/pagealloc"

type (
	LogVersion uint64 // is a monotonic op-log generation (transaction or batch id space)
	TxId       uint64
	Key        string
	Value      string
)

var (
	EmptyValue Value = ""
)

// OpType classifies a log operation for generic handling.
type OpType uint8

const (
	OpSet OpType = iota
	OpDelete
	OpCommit
)

type KeyValue struct {
	K   Key
	V   Value
	Ver LogVersion
}

type KVLockHandle struct {
	Data KeyValue
	// TODO: handle
}

func (h *KVLockHandle) Close() {
	// TODO: Implement
}

// SetOp records a put of Value for Key at Version within TxId.
type SetOp struct {
	Key     Key
	Value   Value
	Version LogVersion
	TxId    TxId
}

// DeleteOp records a tombstone delete for Key at Version within TxId.
type DeleteOp struct {
	Key     Key
	Version LogVersion
	TxId    TxId
}

// CommitOp records that transaction TxId committed at Version.
type CommitOp struct {
	Version LogVersion
	TxId    TxId
}

// LogRecord is a log entry: SetOp, DeleteOp, or CommitOp.
type LogRecord interface {
	OpType() OpType
	TxID() TxId
	LogVersion() LogVersion
}

func (o SetOp) OpType() OpType         { return OpSet }
func (o SetOp) TxID() TxId             { return o.TxId }
func (o SetOp) LogVersion() LogVersion { return o.Version }

func (o DeleteOp) OpType() OpType         { return OpDelete }
func (o DeleteOp) TxID() TxId             { return o.TxId }
func (o DeleteOp) LogVersion() LogVersion { return o.Version }

func (o CommitOp) OpType() OpType         { return OpCommit }
func (o CommitOp) TxID() TxId             { return o.TxId }
func (o CommitOp) LogVersion() LogVersion { return o.Version }

type Oplog interface {
	Append(op LogRecord) error // only appends, without durability guarantees
	CommitVersion(version LogVersion) error

	CurrentActiveVersion() LogVersion // current active inmemory version
	AcquireNextVersion() LogVersion   // acquires next version for new transaction commit (may differ from current active version)

	// TODO: add deadlines
	LockKey(key Key) (KVLockHandle, error)
	LockKeys(keys []Key) (map[Key]KVLockHandle, error)

	FindOne(key Key, atOrBefore LogVersion) (KeyValue, error)
	Find(keys []Key, atOrBefore LogVersion) (map[Key]KeyValue, error)
}

type oplog struct {
	alloc pagealloc.PageAllocator
}

// Recover() // applys recover alogrithm
// recovery/compaction log algorithm:
// all records within current version are committed
// apply all records within transaction with commit mark are committed
// all records within transaction without commit mark are aborted
// recovery also finds two versions: last commit version and last active version
// [r1(v1), r2(v2), r3(v2), (tx1(v3): r1, r2, *not commit*), (tx2(v4): r3, r4, *commit*)]
// -> last commit version = v4, last active version = v4
