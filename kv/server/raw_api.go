package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(req.GetContext())
	defer reader.Close()
	reply := &kvrpcpb.RawGetResponse{}
	if err != nil {
		reply.Error = err.Error()
		return reply, err
	}
	res, err := reader.GetCF(req.GetCf(), req.GetKey())
	reply.Value = res
	if res == nil {
		reply.NotFound = true
	}
	return reply, err
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	modify := storage.Modify{
		Data: storage.Put{
			Cf:    req.Cf,
			Key:   req.Key,
			Value: req.Value,
		},
	}
	resp := &kvrpcpb.RawPutResponse{}
	_ = server.storage.Write(nil, []storage.Modify{modify})
	return resp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	modify := storage.Modify{
		Data: storage.Put{
			Cf:  req.Cf,
			Key: req.Key,
		},
	}
	resp := &kvrpcpb.RawDeleteResponse{}
	_ = server.storage.Write(nil, []storage.Modify{modify})
	return resp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	iter := reader.IterCF(req.Cf)
	resp := &kvrpcpb.RawScanResponse{}
	count := uint32(0)
	for iter.Seek(req.StartKey); iter.Valid(); iter.Next() {
		if count >= req.GetLimit() {
			break
		}
		value, _ := iter.Item().ValueCopy(nil)
		kvPair := &kvrpcpb.KvPair{
			Key:   iter.Item().KeyCopy(nil),
			Value: value,
		}
		resp.Kvs = append(resp.Kvs, kvPair)
		count++
	}
	iter.Close()
	return resp, nil
}
