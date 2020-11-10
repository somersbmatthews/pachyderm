package index

import (
	"context"
	"strings"
	"testing"

	"github.com/pachyderm/pachyderm/src/client/pkg/require"
	"github.com/pachyderm/pachyderm/src/server/pkg/dbutil"
	"github.com/pachyderm/pachyderm/src/server/pkg/storage/chunk"
	"github.com/pachyderm/pachyderm/src/server/pkg/storage/tracker"
	"github.com/pachyderm/pachyderm/src/server/pkg/uuid"
)

const (
	testPath = "test"
)

func write(tb testing.TB, chunks *chunk.Storage, fileNames []string) *Index {
	iw := NewWriter(context.Background(), chunks, uuid.NewWithoutDashes())
	for _, fileName := range fileNames {
		idx := &Index{
			Path:   fileName,
			FileOp: &FileOp{},
		}
		require.NoError(tb, iw.WriteIndex(idx))
	}
	topIdx, err := iw.Close()
	require.NoError(tb, err)
	return topIdx
}

func actualFiles(tb testing.TB, topIdx *Index, chunks *chunk.Storage, opts ...Option) []string {
	ir := NewReader(chunks, topIdx, opts...)
	result := []string{}
	require.NoError(tb, ir.Iterate(context.Background(), func(idx *Index) error {
		result = append(result, idx.Path)
		return nil
	}))
	return result
}

func expectedFiles(fileNames []string, prefix string) []string {
	result := []string{}
	for _, fileName := range fileNames {
		if strings.HasPrefix(fileName, prefix) {
			result = append(result, fileName)
		}
	}
	return result
}

func pathRange(fileNames []string) *PathRange {
	return &PathRange{
		Lower: fileNames[0],
		Upper: fileNames[len(fileNames)-1],
	}
}

func Check(t *testing.T, permString string) {
	db := dbutil.NewTestDB(t)
	tr := tracker.NewTestTracker(t, db)
	_, chunks := chunk.NewTestStorage(t, db, tr)
	fileNames := Generate(permString)
	averageBits = 12
	topIdx := write(t, chunks, fileNames)
	t.Run("Full", func(t *testing.T) {
		expected := fileNames
		actual := actualFiles(t, topIdx, chunks)
		require.Equal(t, expected, actual)
	})
	t.Run("FirstFile", func(t *testing.T) {
		prefix := fileNames[0]
		expected := []string{prefix}
		actual := actualFiles(t, topIdx, chunks, WithPrefix(prefix))
		require.Equal(t, expected, actual)
		actual = actualFiles(t, topIdx, chunks, WithRange(pathRange(expected)))
		require.Equal(t, expected, actual)
	})
	t.Run("FirstRange", func(t *testing.T) {
		prefix := string(fileNames[0][0])
		expected := expectedFiles(fileNames, prefix)
		actual := actualFiles(t, topIdx, chunks, WithPrefix(prefix))
		require.Equal(t, expected, actual)
		actual = actualFiles(t, topIdx, chunks, WithRange(pathRange(expected)))
		require.Equal(t, expected, actual)
	})
	t.Run("MiddleFile", func(t *testing.T) {
		prefix := fileNames[len(fileNames)/2]
		expected := []string{prefix}
		actual := actualFiles(t, topIdx, chunks, WithPrefix(prefix))
		require.Equal(t, expected, actual)
		actual = actualFiles(t, topIdx, chunks, WithRange(pathRange(expected)))
		require.Equal(t, expected, actual)
	})
	t.Run("MiddleRange", func(t *testing.T) {
		prefix := string(fileNames[len(fileNames)/2][0])
		expected := expectedFiles(fileNames, prefix)
		actual := actualFiles(t, topIdx, chunks, WithPrefix(prefix))
		require.Equal(t, expected, actual)
		actual = actualFiles(t, topIdx, chunks, WithRange(pathRange(expected)))
		require.Equal(t, expected, actual)
	})
	t.Run("LastFile", func(t *testing.T) {
		prefix := fileNames[len(fileNames)-1]
		expected := []string{prefix}
		actual := actualFiles(t, topIdx, chunks, WithPrefix(prefix))
		require.Equal(t, expected, actual)
		actual = actualFiles(t, topIdx, chunks, WithRange(pathRange(expected)))
		require.Equal(t, expected, actual)
	})
	t.Run("LastRange", func(t *testing.T) {
		prefix := string(fileNames[len(fileNames)-1][0])
		expected := expectedFiles(fileNames, prefix)
		actual := actualFiles(t, topIdx, chunks, WithPrefix(prefix))
		require.Equal(t, expected, actual)
		actual = actualFiles(t, topIdx, chunks, WithRange(pathRange(expected)))
		require.Equal(t, expected, actual)
	})
}

func TestSingleLevel(t *testing.T) {
	Check(t, "abc")
}

func TestMultiLevel(t *testing.T) {
	Check(t, "abcdefg")
}
