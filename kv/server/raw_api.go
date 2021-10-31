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
	sr, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	data, _ := sr.GetCF(req.Cf, req.Key)
	return &kvrpcpb.RawGetResponse{Value: data, NotFound: data == nil}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	op := storage.Modify{Data: storage.Put{Key: req.Key, Value: req.Value, Cf: req.Cf}}
	err := server.storage.Write(req.Context, []storage.Modify{op})
	return &kvrpcpb.RawPutResponse{}, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	op := storage.Modify{Data: storage.Delete{Key: req.Key, Cf: req.Cf}}
	err := server.storage.Write(req.Context, []storage.Modify{op})
	return &kvrpcpb.RawDeleteResponse{}, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	sr, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	dbIterator := sr.IterCF(req.Cf)
	dbIterator.Seek(req.StartKey)

	kVs := make([]*kvrpcpb.KvPair, 0)
	for i := 0; i < int(req.GetLimit()); i++ {
		if !dbIterator.Valid() {
			break
		}
		item := dbIterator.Item()
		itemValue, _ := item.Value()
		kVs = append(kVs, &kvrpcpb.KvPair{Key: item.Key(), Value: itemValue})
		dbIterator.Next()
	}
	// fmt.Println(kVs)
	return &kvrpcpb.RawScanResponse{Kvs: kVs}, nil
}
