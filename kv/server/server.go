package server

import (
	"bytes"
	"context"
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/errorpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4B)
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
	server.Latches.WaitForLatches([][]byte{req.Key})
	defer server.Latches.ReleaseLatches([][]byte{req.Key})
	resp := &kvrpcpb.GetResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		resp.RegionError = handleRegionError(err)
		return resp, nil
	}
	txn := mvcc.NewMvccTxn(reader, req.Version)
	lock, err := txn.GetLock(req.Key)
	if err != nil {
		return nil, err
	}
	if lock.IsLockedFor(req.Key, txn.StartTS, resp) {
		return resp, nil
	}
	value, err := txn.GetValue(req.Key)
	if err != nil {
		return nil, err
	}
	if len(value) == 0 {
		resp.NotFound = true
		return resp, nil
	}
	resp.Value = value
	return resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	keys := make([][]byte, 0)
	for i := range req.Mutations {
		keys = append(keys, req.Mutations[i].Key)
	}
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)
	resp := &kvrpcpb.PrewriteResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		resp.RegionError = handleRegionError(err)
		return resp, nil
	}
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	for _, mutation := range req.Mutations {
		write, ts, err := txn.MostRecentWrite(mutation.Key)
		if err != nil {
			return nil, err
		}
		if write != nil {
			if ts > txn.StartTS {
				resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{
					Conflict: &kvrpcpb.WriteConflict{Key: mutation.Key, StartTs: write.StartTS, ConflictTs: ts, Primary: req.PrimaryLock},
				})
				return resp, nil
			}
		}
		lock, err := txn.GetLock(mutation.Key)
		if err != nil {
			return nil, err
		}
		if lock != nil {
			if txn.StartTS != mvcc.TsMax || bytes.Compare(mutation.Key, lock.Primary) == 0 {
				resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{Locked: lock.Info(mutation.Key)})
				return resp, nil
			}
		}
		newLock := &mvcc.Lock{Primary: req.PrimaryLock, Ts: req.StartVersion, Ttl: req.LockTtl, Kind: mvcc.WriteKindFromProto(mutation.Op)}
		txn.PutLock(mutation.Key, newLock)
		txn.PutValue(mutation.Key, mutation.Value)
	}
	_ = server.storage.Write(req.Context, txn.Writes())
	return resp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)
	resp := &kvrpcpb.CommitResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		resp.RegionError = handleRegionError(err)
		return resp, nil
	}
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	for _, key := range req.Keys {
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			return nil, err
		}
		if lock != nil && lock.Ts != txn.StartTS {
			resp.Error = &kvrpcpb.KeyError{
				Retryable: fmt.Sprintf("the key %v is pre-written by a different transaction.", string(key)),
			}
			return resp, nil
		}
		if lock == nil {
			if write != nil && write.Kind == mvcc.WriteKindRollback {
				resp.Error = &kvrpcpb.KeyError{
					Abort: fmt.Sprintf("the transaction already roll back!"),
				}
			}
			return resp, nil
		}
		txn.PutWrite(key, req.CommitVersion, &mvcc.Write{StartTS: txn.StartTS, Kind: mvcc.WriteKindPut})
		txn.DeleteLock(key)
	}
	_ = server.storage.Write(req.Context, txn.Writes())
	return resp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.ScanResponse{}
	reader, err := server.storage.Reader(req.Context)
	defer reader.Close()
	if err != nil {
		resp.RegionError = handleRegionError(err)
		return resp, nil
	}
	txn := mvcc.NewMvccTxn(reader, req.Version)
	scanner := mvcc.NewScanner(req.StartKey, txn)
	defer scanner.Close()
	count := req.Limit
	for count > 0 {
		key, value, err := scanner.Next()
		if err != nil {
			resp.Pairs = append(resp.Pairs, &kvrpcpb.KvPair{Key: key, Error: &kvrpcpb.KeyError{Abort: fmt.Sprintf("error %v", err)}})
			continue
		}
		if len(key) == 0 && len(value) == 0 && err == nil {
			break
		}
		if len(key) != 0 && len(value) == 0 {
			continue
		}
		lock, err := txn.GetLock(key)
		if err != nil {
			resp.Pairs = append(resp.Pairs, &kvrpcpb.KvPair{Key: key, Error: &kvrpcpb.KeyError{Abort: fmt.Sprintf("error %v", err)}})
			continue
		}
		if lock != nil && lock.Ts < txn.StartTS {
			resp.Pairs = append(resp.Pairs, &kvrpcpb.KvPair{Key: key, Error: &kvrpcpb.KeyError{Locked: lock.Info(key)}})
			continue
		}
		resp.Pairs = append(resp.Pairs, &kvrpcpb.KvPair{Key: key, Value: value})
		count--
	}
	return resp, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.CheckTxnStatusResponse{}
	reader, err := server.storage.Reader(req.Context)
	defer reader.Close()
	if err != nil {
		resp.RegionError = handleRegionError(err)
		return resp, nil
	}
	txn := mvcc.NewMvccTxn(reader, req.LockTs)
	lock, err := txn.GetLock(req.PrimaryKey)
	if err != nil {
		return nil, err
	}
	if lock != nil {
		if mvcc.PhysicalTime(lock.Ts)+lock.Ttl < mvcc.PhysicalTime(req.CurrentTs) {
			resp.Action = kvrpcpb.Action_TTLExpireRollback
			txn.PutWrite(req.PrimaryKey, txn.StartTS, &mvcc.Write{StartTS: txn.StartTS, Kind: mvcc.WriteKindRollback})
			txn.DeleteLock(req.PrimaryKey)
			txn.DeleteValue(req.PrimaryKey)
		} else {
			resp.Action = kvrpcpb.Action_NoAction
			resp.LockTtl = lock.Ttl
			return resp, nil
		}
	} else {
		write, _, err := txn.CurrentWrite(req.PrimaryKey)
		if err != nil {
			return nil, err
		}
		if write == nil {
			resp.Action = kvrpcpb.Action_LockNotExistRollback
			txn.PutWrite(req.PrimaryKey, txn.StartTS, &mvcc.Write{StartTS: txn.StartTS, Kind: mvcc.WriteKindRollback})
		} else if write.Kind == mvcc.WriteKindRollback {
			resp.Action = kvrpcpb.Action_NoAction
		} else {
			resp.Action = kvrpcpb.Action_NoAction
			resp.CommitVersion = txn.StartTS
		}
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		resp.RegionError = handleRegionError(err)
	}
	return resp, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.BatchRollbackResponse{}
	reader, err := server.storage.Reader(req.Context)
	defer reader.Close()
	if err != nil {
		resp.RegionError = handleRegionError(err)
		return resp, nil
	}
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	for _, key := range req.Keys {
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			return nil, err
		}
		if lock != nil && lock.Ts == txn.StartTS {
			txn.DeleteLock(key)
			txn.DeleteValue(key)
			txn.PutWrite(key, txn.StartTS, &mvcc.Write{StartTS: txn.StartTS, Kind: mvcc.WriteKindRollback})
		} else if (lock != nil && lock.Ts != txn.StartTS) || lock == nil {
			if write != nil {
				if write.Kind == mvcc.WriteKindRollback {
					// 如果已经回滚了就跳过这个 key
					continue
				} else {
					resp.Error = &kvrpcpb.KeyError{Abort: "abort"}
					return resp, nil
				}
			} else {
				txn.PutWrite(key, txn.StartTS, &mvcc.Write{StartTS: txn.StartTS, Kind: mvcc.WriteKindRollback})
			}
		}
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		resp.RegionError = handleRegionError(err)
	}
	return resp, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.ResolveLockResponse{}
	reader, err := server.storage.Reader(req.Context)
	defer reader.Close()
	if err != nil {
		resp.RegionError = handleRegionError(err)
		return resp, nil
	}
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	iter := txn.Reader.IterCF(engine_util.CfLock)
	defer iter.Close()
	for ; iter.Valid(); iter.Next() {
		key := iter.Item().KeyCopy(nil)
		value, _ := iter.Item().ValueCopy(nil)
		lock, _ := mvcc.ParseLock(value)
		if lock.Ts != txn.StartTS {
			continue
		}
		if req.CommitVersion == 0 {
			txn.DeleteLock(key)
			txn.DeleteValue(key)
			txn.PutWrite(key, txn.StartTS, &mvcc.Write{StartTS: txn.StartTS, Kind: mvcc.WriteKindRollback})
		} else {
			txn.DeleteLock(key)
			txn.PutWrite(key, req.CommitVersion, &mvcc.Write{StartTS: txn.StartTS, Kind: mvcc.WriteKindPut})
		}
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		resp.RegionError = handleRegionError(err)
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

func handleRegionError(err error) *errorpb.Error {
	regionError := &errorpb.Error{}
	switch e := err.(type) {
	case *util.ErrKeyNotInRegion:
		regionError.KeyNotInRegion = &errorpb.KeyNotInRegion{Key: e.Key, RegionId: e.Region.Id, StartKey: e.Region.StartKey, EndKey: e.Region.EndKey}
	case *util.ErrEpochNotMatch:
		regionError.EpochNotMatch = &errorpb.EpochNotMatch{CurrentRegions: e.Regions}
	case *util.ErrNotLeader:
		regionError.NotLeader = &errorpb.NotLeader{RegionId: e.RegionId, Leader: e.Leader}
	case *util.ErrRegionNotFound:
		regionError.RegionNotFound = &errorpb.RegionNotFound{RegionId: e.RegionId}
	case *util.ErrStaleCommand:
		regionError.StaleCommand = &errorpb.StaleCommand{}
	case *util.ErrStoreNotMatch:
		regionError.StoreNotMatch = &errorpb.StoreNotMatch{RequestStoreId: e.RequestStoreId, ActualStoreId: e.ActualStoreId}
	default:
		panic(e)
	}
	return regionError
}
