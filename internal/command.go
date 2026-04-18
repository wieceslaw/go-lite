package internal

import "errors"

type Db struct {
	Oplog       Oplog
	TxAllocator TxAllocator
	TxStates    map[TxId]TxState
	KeyLocker   KeyLocker
}

type KeyLocker interface {
	Lock(key Key) error
	Unlock(key Key) error
}

type TxAllocator interface {
	Allocate() TxId
}

type TxState struct {
	Version LogVersion
	Keys    []Key // keys that were touched in this transaction
}

func (db *Db) Get(key Key) (Value, error) {
	v := db.Oplog.CurrentActiveVersion()
	records, err := db.Oplog.Find([]Key{key}, v)
	if err != nil {
		return "", err
	}
	if len(records) == 0 {
		return "", errors.New("key not found")
	}
	return *records[key].Value, nil
}

func (db *Db) Begin() (TxId, error) {
	return db.TxAllocator.Allocate(), nil
}

func (db *Db) End(txId TxId) error {
	txState, ok := db.TxStates[txId]
	if !ok {
		return errors.New("transaction not found")
	}

	records, err := db.Oplog.Find(txState.Keys, txState.Version)
	if err != nil {
		return err
	}

	for _, key := range txState.Keys {
		if records[key].Version != txState.Version {
			return errors.New("transaction aborted")
		}
	}

	return db.Oplog.CommitVersion(txState.Version)
}
