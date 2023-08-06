package rdlock

import (
	"context"
	"github.com/redis/go-redis/v9"
	"time"
)

// RenewableDistributeLock renewable distribute lock
type RenewableDistributeLock struct {
	*DistributeLock
}

func NewRenewableDistributeLock(rds *redis.Client, key string, expiredSecond int) *RenewableDistributeLock {
	return &RenewableDistributeLock{DistributeLock: NewDistributeLock(rds, key, expiredSecond)}
}

func (l *RenewableDistributeLock) Lock(ctx context.Context) (ok bool, err error) {
	ok, err = l.DistributeLock.Lock(ctx)
	if err != nil {
		return ok, err
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Duration(l.expiration*2/3) * time.Second):
				ok, err := l.renew(ctx)
				if err != nil {
					// log
				}
				if !ok {
					return
				}
			}
		}
	}()
	return ok, nil
}
