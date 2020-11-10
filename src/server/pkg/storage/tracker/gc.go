package tracker

import (
	"context"
	"time"

	"github.com/pachyderm/pachyderm/src/client/pkg/errors"
	"github.com/sirupsen/logrus"
)

type Deleter interface {
	Delete(ctx context.Context, id string) error
}

type DeleterMux func(string) Deleter

func (dm DeleterMux) Delete(ctx context.Context, id string) error {
	deleter := dm(id)
	if deleter == nil {
		return errors.Errorf("deleter mux does not have deleter for (%s)", id)
	}
	return deleter.Delete(ctx, id)
}

type GarbageCollector struct {
	tracker Tracker
	period  time.Duration
	deleter Deleter
}

func NewGarbageCollector(tracker Tracker, period time.Duration, deleter Deleter) *GarbageCollector {
	return &GarbageCollector{
		tracker: tracker,
		period:  period,
		deleter: deleter,
	}
}

func (gc *GarbageCollector) Run(ctx context.Context) error {
	ticker := time.NewTicker(gc.period)
	defer ticker.Stop()
	for {
		ctx, cf := context.WithTimeout(ctx, gc.period/2)
		if err := gc.runUntilEmpty(ctx); err != nil {
			logrus.Error(err)
		}
		cf()
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (gc *GarbageCollector) runUntilEmpty(ctx context.Context) error {
	for {
		n, err := gc.runOnce(ctx)
		if err != nil {
			return err
		}
		if n == 0 {
			break
		}
	}
	return nil
}

func (gc *GarbageCollector) runOnce(ctx context.Context) (int, error) {
	var n int
	err := gc.tracker.IterateDeletable(ctx, func(id string) error {
		if err := gc.deleteObject(ctx, id); err != nil {
			logrus.Errorf("error deleting object (%s): %v", id, err)
		} else {
			n++
		}
		return nil
	})
	return n, err
}

func (gc *GarbageCollector) deleteObject(ctx context.Context, id string) error {
	if err := gc.tracker.MarkTombstone(ctx, id); err != nil {
		return err
	}
	if err := gc.deleter.Delete(ctx, id); err != nil {
		return err
	}
	if err := gc.tracker.FinishDelete(ctx, id); err != nil {
		return err
	}
	return nil
}
