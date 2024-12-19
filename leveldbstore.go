// Package leveldbstore implements the [blob.StoreCloser] interface on LevelDB.
package leveldbstore

import (
	"bytes"
	"context"
	"errors"
	"sync"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/storage/dbkey"
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
	*dbMonitor
}

// New opens a LevelDB database at path and returns a store associated with
// that database.
func New(path string, opts *Options) (Store, error) {
	db, err := leveldb.OpenFile(path, opts.openOptions())
	if err != nil {
		return Store{}, err
	}
	return Store{dbMonitor: &dbMonitor{
		db:   db,
		subs: make(map[string]*dbMonitor),
		kvs:  make(map[string]KV),
	}}, nil
}

type dbMonitor struct {
	db     *leveldb.DB
	prefix dbkey.Prefix

	μ    sync.Mutex
	subs map[string]*dbMonitor
	kvs  map[string]KV
}

// Keyspace implements a method of [blob.Store].
// A successful result has concrete type [KV].
// This method never reports an error.
func (d *dbMonitor) Keyspace(_ context.Context, name string) (blob.KV, error) {
	d.μ.Lock()
	defer d.μ.Unlock()

	kv, ok := d.kvs[name]
	if !ok {
		kv = KV{
			db:     d.db,
			prefix: d.prefix.Keyspace(name),
		}
		d.kvs[name] = kv
	}
	return kv, nil
}

// Sub implements a method of [blob.Store].
// This method never reports an error.
func (d *dbMonitor) Sub(_ context.Context, name string) (blob.Store, error) {
	d.μ.Lock()
	defer d.μ.Unlock()

	sub, ok := d.subs[name]
	if !ok {
		sub = &dbMonitor{
			db:     d.db,
			prefix: d.prefix.Sub(name),
			subs:   make(map[string]*dbMonitor),
			kvs:    make(map[string]KV),
		}
		d.subs[name] = sub
	}
	return sub, nil
}

// Close implements part of the [blob.StoreCloser] interface, it closes the
// underlying LevelDB file.
func (s Store) Close(_ context.Context) error { return s.db.Close() }

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
	opt := &opt.Options{OpenFilesCacheCapacity: 50, ErrorIfMissing: true}
	if o != nil {
		opt.ErrorIfMissing = !o.Create
	}
	return opt
}

// Get implements the corresponding method of the [blob.KV] interface.
func (s KV) Get(ctx context.Context, key string) ([]byte, error) {
	data, err := s.db.Get([]byte(s.prefix.Add(key)), nil)
	if err == leveldb.ErrNotFound {
		return nil, blob.KeyNotFound(key)
	}
	return data, err
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
func (s KV) List(ctx context.Context, start string, f func(string) error) error {
	pfx := []byte(s.prefix)
	it := s.db.NewIterator(&util.Range{
		Start: []byte(s.prefix.Add(start)),
	}, &opt.ReadOptions{
		DontFillCache: true,
	})
	defer it.Release()
	for it.Next() {
		if !bytes.HasPrefix(it.Key(), pfx) {
			break // no more keys in this range
		}
		dkey := s.prefix.Remove(string(it.Key()))
		if err := f(dkey); errors.Is(err, blob.ErrStopListing) {
			return nil
		} else if err != nil {
			return err
		}

		if err := ctx.Err(); err != nil {
			return err
		}
	}
	return it.Error()
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
