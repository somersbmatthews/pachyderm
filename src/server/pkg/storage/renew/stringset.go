package renew

import (
	"context"
	"sync"
	"time"
)

type RenewStringFunc func(ctx context.Context, x string, ttl time.Duration) error

type StringSet struct {
	*Renewer
	mu  sync.Mutex
	set map[string]struct{}
}

func NewStringSet(ttl time.Duration, renewFunc RenewStringFunc) *StringSet {
	ss := &StringSet{
		set: map[string]struct{}{},
	}
	ss.Renewer = NewRenewer(ttl, func(ctx context.Context, ttl time.Duration) error {
		ss.mu.Lock()
		defer ss.mu.Unlock()
		for s := range ss.set {
			if err := renewFunc(ctx, s, ttl); err != nil {
				return err
			}
		}
		return nil
	})
	return ss
}

// Add adds x to the set of strings being renewed
func (ss *StringSet) Add(x string) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.set[x] = struct{}{}
}

// Remove removes x from the set of strings being renewed
func (ss *StringSet) Remove(x string) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	delete(ss.set, x)
}

// WithStringSet creates a StringSet using ttl and rf. It calls cb with the StringSets context and the new StringSet.
// If ctx is cancelled, the StringSet will be Closed, and the cancellation will propagate down to the context passed to cb.
func WithStringSet(ctx context.Context, ttl time.Duration, rf RenewStringFunc, cb func(ctx context.Context, ss *StringSet) error) error {
	ss := NewStringSet(ttl, rf)
	defer ss.Close()
	go func() {
		select {
		case <-ctx.Done():
			ss.Close()
		case <-ss.Context().Done():
		}
	}()
	if err := cb(ss.Context(), ss); err != nil {
		return err
	}
	return ss.Close()
}
