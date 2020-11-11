package fileset

import (
	"time"

	"github.com/pachyderm/pachyderm/src/server/pkg/storage/fileset/index"
	"github.com/pachyderm/pachyderm/src/server/pkg/storage/renew"
	"golang.org/x/sync/semaphore"
)

// StorageOption configures a storage.
type StorageOption func(s *Storage)

// WithMemoryThreshold sets the memory threshold that must
// be met before a file set part is serialized (excluding close).
func WithMemoryThreshold(threshold int64) StorageOption {
	return func(s *Storage) {
		s.memThreshold = threshold
	}
}

// WithShardThreshold sets the size threshold that must
// be met before a shard is created by the shard function.
func WithShardThreshold(threshold int64) StorageOption {
	return func(s *Storage) {
		s.shardThreshold = threshold
	}
}

// WithLevelZeroSize sets the size for level zero in the compacted
// representation of a file set.
func WithLevelZeroSize(size int64) StorageOption {
	return func(s *Storage) {
		s.levelZeroSize = size
	}
}

// WithLevelSizeBase sets the base of the exponential growth function
// for level sizes in the compacted representation of a file set.
func WithLevelSizeBase(base int) StorageOption {
	return func(s *Storage) {
		s.levelSizeBase = base
	}
}

// WithMaxOpenFileSets sets the maximum number of filesets that will be open
// (potentially buffered in memory) at a time.
func WithMaxOpenFileSets(max int) StorageOption {
	return func(s *Storage) {
		s.filesetSem = semaphore.NewWeighted(int64(max))
	}
}

// UnorderedWriterOption configures an UnorderedWriter.
type UnorderedWriterOption func(f *UnorderedWriter)

// WithRenewal configures the UnorderedWriter to renew subfileset paths
// with the provided renewer.
func WithRenewal(ttl time.Duration, r *renew.StringSet) UnorderedWriterOption {
	return func(uw *UnorderedWriter) {
		uw.ttl = ttl
		uw.renewer = r
	}
}

// WriterOption configures a file set writer.
type WriterOption func(w *Writer)

// WithNoUpload sets the writer to no upload (will not upload chunks).
func WithNoUpload() WriterOption {
	return func(w *Writer) {
		w.noUpload = true
	}
}

// WithIndexCallback sets a function to be called after each index is written.
// If WithNoUpload is set, the function is called after the index would have been written.
func WithIndexCallback(cb func(*index.Index) error) WriterOption {
	return func(w *Writer) {
		w.indexFunc = cb
	}
}

// WithTTL sets the ttl for the fileset
func WithTTL(ttl time.Duration) WriterOption {
	return func(w *Writer) {
		w.ttl = ttl
	}
}
