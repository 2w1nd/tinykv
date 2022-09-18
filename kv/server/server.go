package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4A/4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.GetResponse{
		RegionError: nil,
		Error:       nil,
		Value:       nil,
		NotFound:    false,
	}

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	// 为所有的key上锁
	server.Latches.AcquireLatches([][]byte{req.Key})
	defer server.Latches.ReleaseLatches([][]byte{req.Key})

	txn := mvcc.NewMvccTxn(reader, req.Version)
	lock, err := txn.GetLock(req.Key)
	if err != nil {
		return nil, err
	}
	if lock != nil && lock.Ts <= req.Version { // 存在之前尚未commit的请求
		resp.Error = &kvrpcpb.KeyError{
			Locked: &kvrpcpb.LockInfo{
				PrimaryLock: lock.Primary,
				LockVersion: lock.Ts,
				Key:         req.Key,
				LockTtl:     lock.Ttl,
			},
		}
		return resp, nil
	}
	val, err := txn.GetValue(req.Key)
	if err != nil {
		return nil, err
	}
	if val == nil {
		resp.NotFound = true
	} else {
		resp.Value = val
	}
	return resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.PrewriteResponse{
		RegionError: nil,
		Errors:      nil,
	}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	server.Latches.AcquireLatches([][]byte{req.PrimaryLock})
	defer server.Latches.ReleaseLatches([][]byte{req.PrimaryLock})

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	errors := make([]*kvrpcpb.KeyError, 0)
	for _, mutation := range req.Mutations {
		write, ts, err := txn.MostRecentWrite(mutation.Key)
		if err != nil {
			return nil, err
		}
		// 最近一些写入的commit ts大于本次事务ts，存在写写冲突
		if write != nil && ts >= txn.StartTS {
			errors = append(errors, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    txn.StartTS,
					ConflictTs: ts,
					Key:        mutation.Key,
					Primary:    req.PrimaryLock,
				},
			})
			resp.Errors = errors
			return resp, nil
		}

		lock, err := txn.GetLock(mutation.Key)
		if err != nil {
			return nil, err
		}
		// 当前key被占用
		if lock != nil {
			errors = append(errors, &kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					PrimaryLock: lock.Primary,
					LockVersion: lock.Ts,
					Key:         mutation.Key,
					LockTtl:     req.LockTtl,
				},
			})
			resp.Errors = errors
			return resp, nil
		}
	}

	// 进行写入
	for _, mutation := range req.Mutations {
		key := mutation.Key
		val := mutation.Value
		lock := mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      txn.StartTS,
			Ttl:     req.LockTtl,
			Kind:    0,
		}
		switch mutation.Op {
		case kvrpcpb.Op_Put:
			txn.PutValue(key, val)
			lock.Kind = mvcc.WriteKindPut
			txn.PutLock(key, &lock)
		case kvrpcpb.Op_Del:
			txn.DeleteValue(key)
			lock.Kind = mvcc.WriteKindDelete
			txn.PutLock(key, &lock)
		default:
			panic("This is not should append")
		}
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.CommitResponse{
		RegionError: nil,
		Error:       nil,
	}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	server.Latches.AcquireLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	for _, key := range req.Keys {
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock == nil {
			return resp, nil
		}
		if lock.Ts != txn.StartTS {
			resp.Error = &kvrpcpb.KeyError{Retryable: "true"}
			return resp, nil
		}
	}

	// 写入write，删除lock
	for _, key := range req.Keys {
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		txn.PutWrite(key, req.CommitVersion, &mvcc.Write{
			StartTS: txn.StartTS,
			Kind:    lock.Kind,
		})
		txn.DeleteLock(key)
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.ScanResponse{
		RegionError: nil,
		Pairs:       nil,
	}

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.Version)
	scan := mvcc.NewScanner(req.StartKey, txn)
	defer scan.Close()
	for i := uint32(0); i < req.Limit; i++ {
		key, val, err := scan.Next()
		if key == nil && val == nil && err == nil {
			break
		}
		if err != nil {
			return nil, err
		}

		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock != nil && lock.Ts <= req.Version {
			keyError := &kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					PrimaryLock: lock.Primary,
					LockVersion: lock.Ts,
					Key:         key,
					LockTtl:     lock.Ttl,
				},
			}
			resp.Pairs = append(resp.Pairs, &kvrpcpb.KvPair{
				Error: keyError,
				Key:   key,
				Value: nil,
			})
			continue
		}
		if val == nil {
			continue
		}
		resp.Pairs = append(resp.Pairs, &kvrpcpb.KvPair{
			Error: nil,
			Key:   key,
			Value: val,
		})
	}
	return resp, nil
}

// KvCheckTxnStatus 主要检查当前锁的状态
func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.CheckTxnStatusResponse{
		RegionError:   nil,
		LockTtl:       0,
		CommitVersion: 0,
		Action:        0,
	}

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.LockTs)
	write, ts, err := txn.CurrentWrite(req.PrimaryKey)
	if err != nil {
		return nil, err
	}
	// 如果不是 WriteKindRollback，则说明已经被 commit，不用管了，返回其 commitTs
	if write != nil && write.Kind != mvcc.WriteKindRollback {
		resp.CommitVersion = ts
		return resp, nil
	}
	lock, err := txn.GetLock(req.PrimaryKey)
	if lock == nil {
		// 没有 lock，说明 primary key 已经被回滚了，创建一个 WriteKindRollback 并直接返回
		txn.DeleteValue(req.PrimaryKey)
		txn.PutWrite(req.PrimaryKey, txn.StartTS, &mvcc.Write{
			StartTS: txn.StartTS,
			Kind:    mvcc.WriteKindRollback,
		})
		resp.Action = kvrpcpb.Action_LockNotExistRollback
	} else {
		// 超时，移除该 Lock 和 Value，并创建一个 WriteKindRollback 标记回滚
		if mvcc.PhysicalTime(lock.Ts)+lock.Ttl < mvcc.PhysicalTime(req.CurrentTs) {
			txn.DeleteLock(req.PrimaryKey)
			txn.DeleteValue(req.PrimaryKey)
			txn.PutWrite(req.PrimaryKey, txn.StartTS, &mvcc.Write{
				StartTS: txn.StartTS,
				Kind:    mvcc.WriteKindRollback,
			})
			resp.Action = kvrpcpb.Action_TTLExpireRollback
		} else { // 直接返回，等待 Lock 超时为止
			resp.LockTtl = lock.Ttl
			return resp, nil
		}
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.BatchRollbackResponse{
		RegionError: nil,
		Error:       nil,
	}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	for _, key := range req.Keys {
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			return nil, err
		}
		// 这个 key 已经被回滚完毕，跳过这个 key
		if write != nil {
			if write.Kind != mvcc.WriteKindRollback { // 注意，如果不是也要回滚
				resp.Error = &kvrpcpb.KeyError{Abort: "true"}
				return resp, nil
			}
			continue
		}
		lock, err := txn.GetLock(key)
		if lock != nil && lock.Ts > txn.StartTS {
			resp.Error = &kvrpcpb.KeyError{Abort: "true"}
			return resp, nil
		}
		// 只允许删除自己事务的lock
		if lock != nil && lock.Ts == txn.StartTS {
			txn.DeleteLock(key)
		}
		txn.DeleteValue(key)
		txn.PutWrite(key, txn.StartTS, &mvcc.Write{
			StartTS: txn.StartTS,
			Kind:    mvcc.WriteKindRollback,
		})
	}

	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// KvResolveLock 用于解决锁冲突，检查primary key的状态，并通过CommitVersion决定是全部回滚还是提交
func (server *Server) KvResolveLock(ctx context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.ResolveLockResponse{
		RegionError: nil,
		Error:       nil,
	}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	keys := make([][]byte, 0)
	iter := reader.IterCF(engine_util.CfLock)
	defer iter.Close()
	for ; iter.Valid(); iter.Next() {
		item := iter.Item()
		val, err := item.ValueCopy(nil)
		if err != nil {
			return nil, err
		}
		lock, err := mvcc.ParseLock(val)
		if err != nil {
			return nil, err
		}
		if lock.Ts == req.StartVersion {
			keys = append(keys, item.KeyCopy(nil))
		}
	}
	if len(keys) == 0 {
		return resp, nil
	}
	// rollback all locks
	if req.CommitVersion == 0 {
		rollBack, err := server.KvBatchRollback(ctx, &kvrpcpb.BatchRollbackRequest{
			Context:      req.Context,
			StartVersion: req.StartVersion,
			Keys:         keys,
		})
		if err != nil {
			return nil, err
		}
		resp.Error = rollBack.Error
		resp.RegionError = rollBack.RegionError
	} else { // commit
		commit, err := server.KvCommit(ctx, &kvrpcpb.CommitRequest{
			Context:       req.Context,
			StartVersion:  req.StartVersion,
			Keys:          keys,
			CommitVersion: req.CommitVersion,
		})
		if err != nil {
			return nil, err
		}
		resp.Error = commit.Error
		resp.RegionError = commit.RegionError
	}
	return resp, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
