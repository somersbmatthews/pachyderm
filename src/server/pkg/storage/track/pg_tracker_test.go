package track

import (
	"testing"

	_ "github.com/lib/pq"
	"github.com/pachyderm/pachyderm/src/server/pkg/dbutil"
)

func TestPostgresTracker(t *testing.T) {
	t.Parallel()
	TestTracker(t, func(testing.TB) Tracker {
		db := dbutil.NewTestDB(t)
		db.MustExec("CREATE SCHEMA storage")
		db.MustExec(schema)
		return NewPostgresTracker(db)
	})
}
