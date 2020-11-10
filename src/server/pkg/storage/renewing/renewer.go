package renewing

import (
	"context"
	"errors"
	"time"
)

type RenewFunc func(ctx context.Context, ttl time.Duration) error

type VeryGenericRenewer struct {
	renewFunc RenewFunc
	ttl       time.Duration

	cancel context.CancelFunc
	done   chan struct{}
	err    error
}

func NewVeryGenericRenewer(ttl time.Duration, renewFunc RenewFunc) *VeryGenericRenewer {
	ctx, cancel := context.WithCancel(context.Background())
	r := &VeryGenericRenewer{
		renewFunc: renewFunc,
		ttl:       ttl,

		cancel: cancel,
		done:   make(chan struct{}),
	}
	go func() {
		r.err = r.renewLoop(ctx)
		close(r.done)
	}()
	return r
}

func (r *VeryGenericRenewer) IsClosed() bool {
	select {
	case <-r.done:
		return true
	default:
		return false
	}
}

func (r *VeryGenericRenewer) Close() error {
	r.cancel()
	<-r.done
	return r.err
}

func (r *VeryGenericRenewer) renewLoop(ctx context.Context) (retErr error) {
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
