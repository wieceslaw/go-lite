package internal

type (
	LogVersion uint64 // is a monotonic op-log generation (transaction or batch id space)
	TxId       uint64
	Key        string
	Value      string
)

// LogRecord is one logical change at a given log version.
// Value == nil means tombstone (delete).
type LogRecord struct {
	Key     Key
	Value   *Value
	Version LogVersion
	TxId    TxId
	Commit  bool
}

type Oplog interface {
	Append(record LogRecord) error // only appends, without durability guarantees
	CommitVersion(version LogVersion) error

	CurrentActiveVersion() LogVersion // current active inmemory version
	AcquireNextVersion() LogVersion   // acquires next version for new transaction commit (may differ from current active version)

	Find(keys []Key, atOrBefore LogVersion) (map[Key]LogRecord, error)
}

// Recover() // applys recover alogrithm
// recovery/compaction log algorithm:
// all records within current version are committed
// apply all records within transaction with commit mark are committed
// all records within transaction without commit mark are aborted
// recovery also finds two versions: last commit version and last active version
// [r1(v1), r2(v2), r3(v2), (tx1(v3): r1, r2, *not commit*), (tx2(v4): r3, r4, *commit*)]
// -> last commit version = v4, last active version = v4
