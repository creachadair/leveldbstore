package leveldbstore_test

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	"github.com/creachadair/ffs/blob/storetest"
	"github.com/creachadair/leveldbstore"
)

func TestStore(t *testing.T) {
	path, err := ioutil.TempDir("", "leveldbstore")
	if err != nil {
		t.Fatalf("Creating temp directory: %v", err)
	}
	defer os.RemoveAll(path)
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
