// Package leveldbstore implements the [blob.KV] interface using LevelDB.
package leveldbstore

import (
	"context"

	"github.com/creachadair/ffs/blob"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// Opener constructs a leveldbstore from an address comprising a path, for use
// with the store package.
func Opener(_ context.Context, addr string) (blob.KV, error) {
	return New(addr, &Options{Create: true})
}

// A KV implements the [blob.KV] interface backed by a LevelDB file.
type KV struct {
	db *leveldb.DB
}

// New opens a LevelDB database at path and returns a store associated with
// that database.
func New(path string, opts *Options) (*KV, error) {
	db, err := leveldb.OpenFile(path, opts.openOptions())
	if err != nil {
		return nil, err
	}
	return &KV{db: db}, nil
}

// Options provide optional settings for opening and creating a [KV].
type Options struct {
	Create bool // create the database if it does not exist
}

func (o *Options) openOptions() *opt.Options {
	opt := &opt.Options{OpenFilesCacheCapacity: 50, ErrorIfMissing: true}
	if o != nil {
		opt.ErrorIfMissing = !o.Create
	}
	return opt
}

// Close closes the underlying LevelDB file.
func (s *KV) Close(_ context.Context) error { return s.db.Close() }

// Get implements the corresponding method of the [blob.KV] interface.
func (s *KV) Get(ctx context.Context, key string) ([]byte, error) {
	data, err := s.db.Get([]byte(key), nil)
	if err == leveldb.ErrNotFound {
		return nil, blob.KeyNotFound(key)
	}
	return data, err
}

// Put implements the corresponding method of the [blob.KV] interface.
func (s *KV) Put(ctx context.Context, opts blob.PutOptions) error {
	// For replacement we do not require a transaction.
	if opts.Replace {
		return s.db.Put([]byte(opts.Key), opts.Data, nil)
	}
	tr, err := s.db.OpenTransaction()
	if err != nil {
		return err
	}
	defer tr.Discard()
	if ok, err := tr.Has([]byte(opts.Key), nil); err != nil {
		return err
	} else if ok {
		return blob.KeyExists(opts.Key)
	}
	if err := tr.Put([]byte(opts.Key), opts.Data, nil); err != nil {
		return err
	}
	return tr.Commit()
}

// Delete implements the corresponding method of the [blob.KV] interface.
func (s *KV) Delete(ctx context.Context, key string) error {
	tr, err := s.db.OpenTransaction()
	if err != nil {
		return err
	}
	defer tr.Discard()
	if ok, err := tr.Has([]byte(key), nil); err != nil {
		return err
	} else if !ok {
		return blob.KeyNotFound(key)
	}
	if err := tr.Delete([]byte(key), nil); err != nil {
		return err
	}
	return tr.Commit()
}

// List implements the corresponding method of the [blob.KV] interface.
func (s *KV) List(ctx context.Context, start string, f func(string) error) error {
	it := s.db.NewIterator(&util.Range{Start: []byte(start)}, nil)
	defer it.Release()
	for it.Next() {
		if err := f(string(it.Key())); err == blob.ErrStopListing {
			return nil
		} else if err != nil {
			return err
		} else if err := ctx.Err(); err != nil {
			return err
		}
	}
	return it.Error()
}

// Len implements the corresponding method of the [blob.KV] interface.
func (s *KV) Len(ctx context.Context) (int64, error) {
	var n int64
	it := s.db.NewIterator(nil, nil)
	defer it.Release()
	for it.Next() {
		n++
	}
	return n, it.Error()
}
