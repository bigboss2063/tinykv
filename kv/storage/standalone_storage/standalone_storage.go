package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	DB *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	opts := badger.DefaultOptions
	opts.Dir = conf.DBPath
	opts.ValueDir = conf.DBPath
	db, err := badger.Open(opts)
	if err != nil {
		return nil
	}
	return &StandAloneStorage{
		DB: db,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.DB.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	var ssr storage.StandaloneStorageReader
	txn := s.DB.NewTransaction(false)
	ssr = storage.NewStandaloneStorageReader(txn)
	return ssr, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	txn := s.DB.NewTransaction(true)
	for _, modify := range batch {
		if len(modify.Value()) == 0 {
			err := txn.Delete(engine_util.KeyWithCF(modify.Cf(), modify.Key()))
			if err != nil {
				return err
			}
		} else {
			err := txn.Set(engine_util.KeyWithCF(modify.Cf(), modify.Key()), modify.Value())
			if err != nil {
				return err
			}
		}
	}
	return txn.Commit()
}
