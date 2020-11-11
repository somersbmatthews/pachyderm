package track

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pachyderm/pachyderm/src/server/pkg/storage/renew"
	"github.com/pachyderm/pachyderm/src/server/pkg/uuid"
)

type Renewer struct {
	id      string
	tracker Tracker
	ttl     time.Duration
	r       *renew.Renewer

	mu sync.Mutex
	n  int
}

func NewRenewer(tracker Tracker, name string, ttl time.Duration) *Renewer {
	r := &Renewer{
		id:      "tmp-" + name + "-" + uuid.NewWithoutDashes(),
		tracker: tracker,
		ttl:     ttl,
	}
	r.r = renew.NewRenewer(ttl, func(ctx context.Context, ttl time.Duration) error {
		_, err := r.tracker.SetTTLPrefix(ctx, r.id+"/", ttl)
		return err
	})
	return r
}

func (r *Renewer) Add(ctx context.Context, id string) error {
	n := r.nextInt()
	id2 := fmt.Sprintf("%s/%d", r.id, n)
	// create an object whos sole purpose is to reference id, and to have a structured name
	// which can be renewed in bulk by prefix
	return r.tracker.CreateObject(ctx, id2, []string{id}, r.ttl)
}

func (r *Renewer) Close() error {
	return r.r.Close()
}

func (r *Renewer) nextInt() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := r.n
	r.n++
	return n
}
