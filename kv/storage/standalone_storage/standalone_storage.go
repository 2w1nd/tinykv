package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	db   *badger.DB
	opts *badger.Options
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	var standAloneStorage *StandAloneStorage
	opts := badger.DefaultOptions
	opts.Dir = conf.DBPath
	opts.ValueDir = conf.DBPath
	standAloneStorage = new(StandAloneStorage)
	standAloneStorage.opts = &opts
	return standAloneStorage
}

func (s *StandAloneStorage) Start() error {
	tmpdb, err := badger.Open(*s.opts)
	if err != nil {
		log.Fatal("badger db start fail", zap.Error(err))
		return err
	}
	s.db = tmpdb
	return nil
}

func (s *StandAloneStorage) Stop() error {
	err := s.db.Close()
	if err != nil {
		log.Fatal("badger db close fail", zap.Error(err))
		return err
	}
	return nil
}

type StandaloneStorageReader struct {
	txn *badger.Txn
}

func (sr *StandaloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(sr.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (sr *StandaloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	iter := engine_util.NewCFIterator(cf, sr.txn)
	return iter
}

func (sr *StandaloneStorageReader) Close() {
	sr.txn.Discard()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	return &StandaloneStorageReader{s.db.NewTransaction(false)}, nil
}

// 逐步遍历每一个元素，写入badger中的同一个txn
func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	for _, v := range batch {
		switch v.Data.(type) {
		case storage.Put:
			err := engine_util.PutCF(s.db, v.Cf(), v.Key(), v.Value())
			if err != nil {
				return err
			}
		case storage.Delete:
			err := engine_util.DeleteCF(s.db, v.Cf(), v.Key())
			if err != nil {
				return err
			}
		}
	}
	return nil
}
