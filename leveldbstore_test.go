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
