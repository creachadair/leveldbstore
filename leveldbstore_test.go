package leveldbstore_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/creachadair/ffs/blob/storetest"
	"github.com/creachadair/leveldbstore"
)

func TestKV(t *testing.T) {
	path := filepath.Join(t.TempDir(), "leveldbstore")
	t.Logf("Database path: %q", path)

	s, err := leveldbstore.New(path, &leveldbstore.Options{
		Create: true,
	})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer s.Close(context.Background())
	storetest.Run(t, s)
}
