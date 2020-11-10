package dbutil

import (
	"fmt"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/pachyderm/pachyderm/src/client/pkg/require"
)

// set this to true if you want to keep the database around
var devDontDropDatabase = false

const (
	DefaultPostgresHost = "127.0.0.1"
	DefaultPostgresPort = 32228
	TestPostgresUser    = "postgres"
)

// NewTestDB connects to postgres using the default settings, creates a database with a unique name
// then calls cb with a sqlx.DB configured to use the newly created database.
// After cb returns the database is dropped.
func NewTestDB(t testing.TB) *sqlx.DB {
	dsn := fmt.Sprintf("host=%s port=%d user=%s sslmode=disable", DefaultPostgresHost, DefaultPostgresPort, TestPostgresUser)
	db := sqlx.MustOpen("postgres", dsn)
	dbName := fmt.Sprintf("test_%d", time.Now().UnixNano())
	db.MustExec("CREATE DATABASE " + dbName)
	t.Log("database", dbName, "successfully created")
	t.Cleanup(func() {
		if !devDontDropDatabase {
			db.MustExec("DROP DATABASE " + dbName)
		}
		require.NoError(t, db.Close())
	})
	db2 := sqlx.MustOpen("postgres", dsn+" dbname="+dbName)
	t.Cleanup(func() {
		require.NoError(t, db2.Close())
	})
	return db2
}

type DBParams struct {
	Host       string
	Port       int
	User, Pass string
	DBName     string
}

func NewDB(x DBParams) (*sqlx.DB, error) {
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", x.Host, x.Port, x.User, x.Pass, x.DBName)
	return sqlx.Open("postgres", dsn)
}
