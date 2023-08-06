package rdlock

import (
	"context"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

const (
	_testLockKey = "test_lock_key"
)

func TestDistributeLock(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379",
		Password: "123456",
	})
	defer func() {
		err := client.Close()
		if err != nil {
			panic(err)
		}
	}()
	dLock := NewDistributeLock(client, _testLockKey, 10)

	ok, err := dLock.Unlock(ctx) // without lock, unlock shall fail
	assert.Nil(t, err)
	assert.False(t, ok)

	ok, err = dLock.Lock(ctx) // lock success
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = dLock.Lock(ctx) // duplicate lock
	assert.Nil(t, err)
	assert.False(t, ok)

	time.Sleep(time.Second * 10)
	ok, err = dLock.Lock(ctx) // lock timeout, lock shall ok
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = dLock.Unlock(ctx)
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = dLock.Unlock(ctx)
	assert.Nil(t, err)
	assert.False(t, ok)
}

func TestRenewableDistributeLock(t *testing.T) {
	bCtx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379",
		Password: "123456",
	})
	defer func() {
		err := client.Close()
		if err != nil {
			panic(err)
		}
	}()
	dLock := NewRenewableDistributeLock(client, _testLockKey, 10)

	ctx, cancelFunc := context.WithCancel(bCtx)
	ok, err := dLock.Unlock(ctx) // without lock, unlock shall fail
	assert.Nil(t, err)
	assert.False(t, ok)

	ok, err = dLock.Lock(ctx) // lock success
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = dLock.Lock(ctx) // duplicate lock
	assert.Nil(t, err)
	assert.False(t, ok)

	time.Sleep(time.Second * 10)
	ok, err = dLock.Lock(ctx) // lock not timeout, lock shall fail
	assert.Nil(t, err)
	assert.False(t, ok)

	ok, err = dLock.Lock(ctx) // lock not timeout, lock shall fail
	assert.Nil(t, err)
	assert.False(t, ok)

	cancelFunc()
	time.Sleep(time.Second * 10)
	ctx, cancelFunc = context.WithCancel(bCtx)
	defer cancelFunc()

	dLock = NewRenewableDistributeLock(client, _testLockKey, 10)
	ok, err = dLock.Lock(ctx)
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = dLock.Unlock(ctx)
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = dLock.Unlock(ctx)
	assert.Nil(t, err)
	assert.False(t, ok)
}

func TestRenewableDistributeUnLock(t *testing.T) {
	bCtx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379",
		Password: "123456",
	})
	defer func() {
		err := client.Close()
		if err != nil {
			panic(err)
		}
	}()
	ctx, cancelFunc := context.WithCancel(bCtx)
	defer cancelFunc()
	dLock := NewRenewableDistributeLock(client, _testLockKey, 30)
	ok, err := dLock.Lock(ctx)
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = dLock.Unlock(ctx)
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = dLock.Unlock(ctx)
	assert.Nil(t, err)
	assert.False(t, ok)
}

func TestNewRenewableDistributeLockRenew(t *testing.T) {
	bCtx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379",
		Password: "123456",
	})
	defer func() {
		err := client.Close()
		if err != nil {
			panic(err)
		}
	}()

	ctx, cancelFunc := context.WithCancel(bCtx)
	defer cancelFunc()

	dLock := NewRenewableDistributeLock(client, _testLockKey, 10)
	ok, err := dLock.Lock(ctx)
	assert.Nil(t, err)
	assert.True(t, ok)

	time.Sleep(time.Second * 30)

	ok, err = dLock.Unlock(ctx)
	assert.Nil(t, err)
	assert.True(t, ok)
}
