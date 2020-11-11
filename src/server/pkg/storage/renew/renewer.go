package renew

import (
	"context"
	"errors"
	"time"
)

type RenewFunc func(ctx context.Context, ttl time.Duration) error

type Renewer struct {
	renewFunc RenewFunc
	ttl       time.Duration

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	err    error
}

func NewRenewer(ttl time.Duration, renewFunc RenewFunc) *Renewer {
	ctx, cancel := context.WithCancel(context.Background())
	r := &Renewer{
		renewFunc: renewFunc,
		ttl:       ttl,

		ctx:    ctx,
		cancel: cancel,
		done:   make(chan struct{}),
	}
	go func() {
		r.err = r.renewLoop(ctx)
		r.cancel()
		close(r.done)
	}()
	return r
}

// Context returns a context which will be cancelled when the renewer is closed
func (r *Renewer) Context() context.Context {
	return r.ctx
}

// IsClosed returns true if the renewer is closed
func (r *Renewer) IsClosed() bool {
	select {
	case <-r.done:
		return true
	default:
		return false
	}
}

// Close closes the renewer, stopping the background renewal. Close is idempotent.
func (r *Renewer) Close() error {
	r.cancel()
	<-r.done
	return r.err
}

func (r *Renewer) renewLoop(ctx context.Context) (retErr error) {
	defer func() {
		if errors.Is(ctx.Err(), context.Canceled) {
			retErr = nil
		}
	}()
	ticker := time.NewTicker(r.ttl / 3)
	defer ticker.Stop()
	for {
		if err := func() error {
			ctx, cf := context.WithTimeout(ctx, r.ttl/3)
			defer cf()
			return r.renewFunc(ctx, r.ttl)
		}(); err != nil {
			return err
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
