package internal

import "errors"

type KeyLocker interface {
	Lock(key Key) error
	Unlock(key Key) error
}

type TxAllocator interface {
	Allocate() TxId
}

type TxOp interface {
	Key() Key
}

type SetTxOp struct {
	K Key
	V Value
}

func (o *SetTxOp) Key() Key {
	return o.K
}

type DelTxOp struct {
	K Key
}

func (o *DelTxOp) Key() Key {
	return o.K
}

type TxSnapEntry struct {
	Key Key
	Val Value
	Ver LogVersion
}

type TxState struct {
	Id       TxId
	Keys     []Key
	Snapshot map[Key]*TxSnapEntry
	Ops      []TxOp
}

type Executor struct {
	Oplog       Oplog
	TxAllocator TxAllocator
	TxStates    map[TxId]*TxState
}

func (e *Executor) Get(key Key) (Value, error) {
	v := e.Oplog.CurrentActiveVersion()
	record, err := e.Oplog.FindOne(key, v)
	if err != nil {
		return "", err
	}
	return record.V, nil
}

func (e *Executor) GetTx(k Key, tx TxId) (Value, error) {
	state, ok := e.TxStates[tx]
	if !ok {
		return EmptyValue, errors.New("Transaction not found")
	}

	// mark key
	var value Value
	entry, ok := state.Snapshot[k]
	if !ok {
		handle, err := e.Oplog.LockKey(k)
		if err != nil {
			return EmptyValue, err
		}
		defer handle.Close()
		state.Snapshot[k] = new(TxSnapEntry{
			Key: k,
			Val: handle.Data.V,
			Ver: handle.Data.Ver,
		})
		state.Keys = append(state.Keys, k)
		value = handle.Data.V
	} else {
		value = entry.Val
	}

	return value, nil
}

// TODO: use context for deadline in tx?
func (e *Executor) SetTx(k Key, v Value, tx TxId) error {
	state, ok := e.TxStates[tx]
	if !ok {
		return errors.New("Transaction not found")
	}

	// mark key
	entry, ok := state.Snapshot[k]
	if !ok {
		handle, err := e.Oplog.LockKey(k)
		if err != nil {
			return err
		}
		defer handle.Close()
		state.Snapshot[k] = new(TxSnapEntry{
			Key: k,
			Val: v,
			Ver: handle.Data.Ver,
		})
		state.Keys = append(state.Keys, k)
	}

	entry.Val = v
	state.Ops = append(state.Ops, &SetTxOp{
		K: k,
		V: v,
	})
	return nil
}

func (e *Executor) Begin() (TxId, error) {
	txId := e.TxAllocator.Allocate()
	e.TxStates[txId] = new(TxState{
		Id:       txId,
		Keys:     nil,
		Snapshot: make(map[Key]*TxSnapEntry),
		Ops:      nil,
	})
	return txId, nil
}

func (e *Executor) End(txId TxId) error {
	txState, ok := e.TxStates[txId]
	if !ok {
		return errors.New("Transaction not found")
	}

	handles, err := e.Oplog.LockKeys(txState.Keys)
	if err != nil {
		return err
	}
	defer func() {
		for _, h := range handles {
			h.Close()
		}
	}()

	for _, key := range txState.Keys {
		kv := handles[key].Data
		if kv.Ver != txState.Snapshot[key].Ver {
			return errors.New("Transaction aborted")
		}
	}

	// TODO: apply txState.Ops to oplog

	return e.Oplog.CommitVersion(e.Oplog.AcquireNextVersion())
}
