package fileset

import (
	"testing"

	"github.com/pachyderm/pachyderm/src/server/pkg/dbutil"
)

func TestPGStore(t *testing.T) {
	StoreTestSuite(t, func(t testing.TB) Store {
		db := dbutil.NewTestDB(t)
		return NewTestStore(t, db)
	})
}
