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
	db     *badger.DB
	dbPath *string
}

type StandAloneStorageReader struct {
	storage *StandAloneStorage
}

func (r *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCF(r.storage.db, cf, key)
	if err != nil {
		return nil, nil
	}
	return val, err
}

func (r *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	txn := r.storage.db.NewTransaction(false)
	iterator := engine_util.NewCFIterator(cf, txn)

	defer txn.Discard()
	defer iterator.Close()
	return iterator
}

func (r *StandAloneStorageReader) Close() {
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{nil, &conf.DBPath}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	dbOptions := badger.DefaultOptions
	dbOptions.Dir = *s.dbPath
	dbOptions.ValueDir = *s.dbPath
	db, err := badger.Open(dbOptions)
	if err != nil {
		return err
	}
	s.db = db
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	err := s.db.Close()
	if err != nil {
		return err
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return &StandAloneStorageReader{s}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, op := range batch {
		switch op.Data.(type) {
		case storage.Put:
			err := engine_util.PutCF(s.db, op.Cf(), op.Key(), op.Value())
			if err != nil {
				return err
			}
		case storage.Delete:
			err := engine_util.DeleteCF(s.db, op.Cf(), op.Key())
			if err != nil {
				return err
			}
		}
	}
	return nil
}
