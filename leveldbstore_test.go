package leveldbstore_test

import (
	"path/filepath"
	"testing"

	"github.com/creachadair/ffs/blob/storetest"
	"github.com/creachadair/leveldbstore"
)

func TestStore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "leveldbstore")
	t.Logf("Database path: %q", path)

	s, err := leveldbstore.New(path, &leveldbstore.Options{
		Create: true,
	})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	storetest.Run(t, s)
}

func BenchmarkStore(b *testing.B) {
	path := filepath.Join(b.TempDir(), "benchmark.db")
	s, err := leveldbstore.New(path, &leveldbstore.Options{Create: true})
	if err != nil {
		b.Fatal(err)
	}
	kv, err := s.KV(b.Context(), "benchmark")
	if err != nil {
		b.Fatalf("KV: %v", err)
	}
	storetest.BenchmarkKV(b, kv)
}
