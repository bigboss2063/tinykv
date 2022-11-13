package mvcc

import (
	"bytes"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	txn        *MvccTxn
	iter       engine_util.DBIterator
	startKey   []byte
	currentKey []byte
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	scanner := &Scanner{
		txn:        txn,
		iter:       txn.Reader.IterCF(engine_util.CfWrite),
		startKey:   startKey,
		currentKey: []byte{},
	}
	scanner.iter.Seek(EncodeKey(startKey, txn.StartTS))
	return scanner
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.iter.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	if !scan.iter.Valid() {
		return nil, nil, nil
	}
	key := scan.iter.Item().KeyCopy(nil)
	for ; scan.iter.Valid(); scan.iter.Next() {
		item := scan.iter.Item()
		key = item.KeyCopy(nil)
		decodedKey := DecodeUserKey(key)
		ts := decodeTimestamp(key)
		if ts <= scan.txn.StartTS && !bytes.Equal(decodedKey, scan.currentKey) {
			writeValue, _ := item.ValueCopy(nil)
			write, err := ParseWrite(writeValue)
			if err != nil {
				return nil, nil, err
			}
			scan.currentKey = decodedKey
			scan.iter.Next()
			if write.Kind == WriteKindDelete || write.Kind == WriteKindRollback {
				return decodedKey, nil, nil
			}
			encodedKey := EncodeKey(decodedKey, write.StartTS)
			value, err := scan.txn.Reader.GetCF(engine_util.CfDefault, encodedKey)
			if err != nil {
				return nil, nil, err
			}
			return decodedKey, value, nil
		}
	}
	return nil, nil, nil
}
