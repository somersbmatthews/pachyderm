package fileset

import (
	"context"
	"sync"
	"time"

	"github.com/pachyderm/pachyderm/src/server/pkg/storage/renewing"
	"golang.org/x/sync/errgroup"
)

// renewFunc is a function that renews a particular path.
type renewFunc func(ctx context.Context, path string, ttl time.Duration) error

// WithRenewer provides a scoped fileset renewer.
func WithRenewer(ctx context.Context, ttl time.Duration, renew renewFunc, cb func(context.Context, *Renewer) error) error {
	r := newRenewer(ttl, renew)
	cancelCtx, cf := context.WithCancel(ctx)
	eg, errCtx := errgroup.WithContext(cancelCtx)
	eg.Go(func() error {
		return r.run(errCtx)
	})
	eg.Go(func() error {
		defer cf()
		return cb(errCtx, r)
	})
	return eg.Wait()
}

// Renewer manages renewing the TTL on a set of paths.
type Renewer struct {
	renew renewFunc
	mu    sync.Mutex
	paths map[string]struct{}
	r     *renewing.VeryGenericRenewer
}

func newRenewer(ttl time.Duration, renew renewFunc) *Renewer {
	r := &Renewer{
		paths: make(map[string]struct{}),
	}
	r.r = renewing.NewVeryGenericRenewer(ttl, func(ctx context.Context, ttl time.Duration) error {
		r.mu.Lock()
		defer r.mu.Unlock()
		for p := range r.paths {
			if err := r.renew(ctx, p, ttl); err != nil {
				return err
			}
		}
		return nil
	})
	return r
}

func (r *Renewer) run(ctx context.Context) error {
	<-ctx.Done()
	return r.r.Close()
}

// Add adds p to the path set.
func (r *Renewer) Add(p string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.paths[p] = struct{}{}
}

// Remove removes p from the path set
func (r *Renewer) Remove(p string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.paths, p)
}
