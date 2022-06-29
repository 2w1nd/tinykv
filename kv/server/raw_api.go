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
	rawGetResp := &kvrpcpb.RawGetResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	val, err := reader.GetCF(req.GetCf(), req.GetKey())
	if val == nil {
		rawGetResp.NotFound = true
		return rawGetResp, err
	}
	rawGetResp.Value = val
	return rawGetResp, err
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Hint: Consider using Storage.Modify to store data to be modified
	put := storage.Put{
		Key:   req.GetKey(),
		Value: req.GetValue(),
		Cf:    req.GetCf(),
	}
	modify := storage.Modify{Data: put}
	modifys := make([]storage.Modify, 0)
	modifys = append(modifys, modify)
	err := server.storage.Write(req.Context, modifys)
	if err != nil {
		return &kvrpcpb.RawPutResponse{}, err
	}
	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Hint: Consider using Storage.Modify to store data to be deleted
	put := storage.Delete{
		Key: req.GetKey(),
		Cf:  req.GetCf(),
	}
	modify := storage.Modify{Data: put}
	modifys := make([]storage.Modify, 0)
	modifys = append(modifys, modify)
	err := server.storage.Write(req.Context, modifys)
	if err != nil {
		return &kvrpcpb.RawDeleteResponse{}, err
	}
	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return &kvrpcpb.RawScanResponse{}, err
	}
	iter := reader.IterCF(req.Cf)
	defer iter.Close()
	kvs := make([]*kvrpcpb.KvPair, 0)
	iter.Seek(req.StartKey)
	for iter.Seek(req.StartKey); len(kvs) < int(req.Limit) && iter.Valid(); iter.Next() {
		item := iter.Item()
		pair := &kvrpcpb.KvPair{}
		pair.Key = item.KeyCopy(nil)
		pair.Value, err = item.ValueCopy(nil)
		if err != nil {
			return &kvrpcpb.RawScanResponse{}, err
		}
		kvs = append(kvs, pair)
	}
	return &kvrpcpb.RawScanResponse{Kvs: kvs}, nil
}
