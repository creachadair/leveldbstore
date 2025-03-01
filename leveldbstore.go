// Package leveldbstore implements the [blob.StoreCloser] interface on LevelDB.
package leveldbstore

import (
	"bytes"
	"context"
	"errors"
	"iter"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/storage/dbkey"
	"github.com/creachadair/ffs/storage/monitor"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// Opener constructs a leveldbstore from an address comprising a path, for use
// with the store package.
func Opener(_ context.Context, addr string) (blob.StoreCloser, error) {
	return New(addr, &Options{Create: true})
}

// Store implements the [blob.StoreCloser] interface over a LevelDB instance.
type Store struct {
	*monitor.M[*leveldb.DB, KV]
}

// New opens a LevelDB database at path and returns a store associated with
// that database.
func New(path string, opts *Options) (Store, error) {
	db, err := leveldb.OpenFile(path, opts.openOptions())
	if err != nil {
		return Store{}, err
	}
	return Store{M: monitor.New(monitor.Config[*leveldb.DB, KV]{
		DB: db,
		NewKV: func(_ context.Context, db *leveldb.DB, pfx dbkey.Prefix, _ string) (KV, error) {
			return KV{db: db, prefix: pfx}, nil
		},
	})}, nil
}

// Close implements part of the [blob.StoreCloser] interface, it closes the
// underlying LevelDB file.
func (s Store) Close(_ context.Context) error { return s.DB.Close() }

// A KV implements the [blob.KV] interface backed by a LevelDB file.
type KV struct {
	db     *leveldb.DB
	prefix dbkey.Prefix
}

// Options provide optional settings for opening and creating a [KV].
type Options struct {
	Create bool // create the database if it does not exist
}

func (o *Options) openOptions() *opt.Options {
	opt := &opt.Options{OpenFilesCacheCapacity: 500, ErrorIfMissing: true}
	if o != nil {
		opt.ErrorIfMissing = !o.Create
	}
	return opt
}

// Get implements the corresponding method of the [blob.KV] interface.
func (s KV) Get(ctx context.Context, key string) ([]byte, error) {
	data, err := s.db.Get([]byte(s.prefix.Add(key)), nil)
	if errors.Is(err, leveldb.ErrNotFound) {
		return nil, blob.KeyNotFound(key)
	}
	return data, err
}

// Has implements the corresponding method of the [blob.KV] interface.
func (s KV) Has(ctx context.Context, keys ...string) (blob.KeySet, error) {
	var out blob.KeySet
	for _, key := range keys {
		ok, err := s.db.Has([]byte(s.prefix.Add(key)), &opt.ReadOptions{DontFillCache: true})
		if err != nil {
			return nil, err
		}
		if ok {
			out.Add(key)
		}
	}
	return out, nil
}

// Put implements the corresponding method of the [blob.KV] interface.
func (s KV) Put(ctx context.Context, opts blob.PutOptions) error {
	ekey := []byte(s.prefix.Add(opts.Key))
	// For replacement we do not require a transaction.
	if opts.Replace {
		return s.db.Put(ekey, opts.Data, nil)
	}
	tr, err := s.db.OpenTransaction()
	if err != nil {
		return err
	}
	defer tr.Discard()

	if ok, err := tr.Has(ekey, nil); err != nil {
		return err
	} else if ok {
		return blob.KeyExists(opts.Key)
	}
	if err := tr.Put(ekey, opts.Data, nil); err != nil {
		return err
	}
	return tr.Commit()
}

// Delete implements the corresponding method of the [blob.KV] interface.
func (s KV) Delete(ctx context.Context, key string) error {
	tr, err := s.db.OpenTransaction()
	if err != nil {
		return err
	}
	defer tr.Discard()

	ekey := []byte(s.prefix.Add(key))
	if ok, err := tr.Has(ekey, nil); err != nil {
		return err
	} else if !ok {
		return blob.KeyNotFound(key)
	}
	if err := tr.Delete(ekey, nil); err != nil {
		return err
	}
	return tr.Commit()
}

// List implements the corresponding method of the [blob.KV] interface.
func (s KV) List(ctx context.Context, start string) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		pfx := []byte(s.prefix)
		it := s.db.NewIterator(&util.Range{
			Start: []byte(s.prefix.Add(start)),
		}, &opt.ReadOptions{
			DontFillCache: true,
		})
		defer it.Release()
		for it.Next() {
			if err := ctx.Err(); err != nil {
				yield("", err)
				return
			}
			if !bytes.HasPrefix(it.Key(), pfx) {
				break // no more keys in this range
			}
			dkey := s.prefix.Remove(string(it.Key()))
			if !yield(dkey, nil) {
				return
			}
		}
		if err := it.Error(); err != nil {
			yield("", err)
		}
	}
}

// Len implements the corresponding method of the [blob.KV] interface.
func (s KV) Len(ctx context.Context) (int64, error) {
	var n int64
	pfx := []byte(s.prefix)
	it := s.db.NewIterator(&util.Range{Start: pfx}, &opt.ReadOptions{DontFillCache: true})
	defer it.Release()
	for it.Next() {
		if !bytes.HasPrefix(it.Key(), pfx) {
			break
		}
		n++
	}
	return n, it.Error()
}
