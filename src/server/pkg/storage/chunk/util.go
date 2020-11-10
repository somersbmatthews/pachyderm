package chunk

import (
	"math/rand"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/pachyderm/pachyderm/src/server/pkg/obj"
	"github.com/pachyderm/pachyderm/src/server/pkg/storage/tracker"
)

// NewTestStorage creates a local storage instance for testing during the lifetime of
// the callback.
func NewTestStorage(t testing.TB, db *sqlx.DB, tr tracker.Tracker, opts ...StorageOption) (obj.Client, *Storage) {
	// metadata store
	db.MustExec(schema)
	mdstore := NewPostgresStore(db)
	// object client
	objC := obj.NewTestClient(t)
	return objC, NewStorage(objC, mdstore, tr, opts...)
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

// RandSeq generates a random sequence of data (n is number of bytes)
func RandSeq(n int) []byte {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return []byte(string(b))
}

// Reference creates a data reference for the full chunk referenced by a data reference.
func Reference(dataRef *DataRef) *DataRef {
	chunkDataRef := &DataRef{}
	chunkDataRef.Ref = dataRef.Ref
	chunkDataRef.SizeBytes = dataRef.Ref.SizeBytes
	return chunkDataRef
}
