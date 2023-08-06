package rdlock

import (
	"context"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
	"strings"
	"time"
)

const (
	_unlockLuaScript = `
if redis.call('get',KEYS[1]) == ARGV[1] then 
   return redis.call('del',KEYS[1]) 
else
   return 0
end;
`
)

type DistributeLock struct {
	rds        *redis.Client
	key        string // redis key
	uid        string // lock uuid, usual represent lock owner's id
	expiration int    // lock expire time
}

func NewDistributeLock(rds *redis.Client, key string, expiredSecond int) *DistributeLock {
	return &DistributeLock{rds: rds, key: key, expiration: expiredSecond}
}

func (d *DistributeLock) Lock(ctx context.Context) (ok bool, err error) {
	logHead := "Lock|"
	uuidObj, err := uuid.NewRandom()
	if err != nil {
		return false, errors.Errorf(logHead+"uuid.NewRandom err:%s", err)
	}
	uid := uuidObj.String()

	res := d.rds.Do(ctx, "set", d.key, uid, "nx", "ex", d.expiration)
	if res.Err() != nil {
		if res.Err() == redis.Nil {
			return false, nil
		}
		return false, errors.Errorf(logHead+"rds.Do err:%s", res.Err())
	}
	resStr := res.Val().(string)
	if strings.ToLower(resStr) == "ok" { // lock success
		d.uid = uid
		return true, nil
	}
	return false, nil
}

func (d *DistributeLock) Unlock(ctx context.Context) (ok bool, err error) {
	logHead := "Unlock|"
	if d.uid == "" {
		// lock fail
		return false, nil
	}
	ok, err = d.rds.Eval(ctx, _unlockLuaScript, []string{d.key}, d.uid).Bool()
	if err != nil {
		if err == redis.Nil {
			return false, nil
		}
		return false, errors.Errorf(logHead+"rds.Eval err:%s", err)
	}
	return ok, nil
}

// renew lock is an inner func used by RenewableDistributeLock
func (d *DistributeLock) renew(ctx context.Context) (bool, error) {
	logHead := "renew|"
	err := d.rds.Expire(ctx, d.key, time.Duration(d.expiration)*time.Second).Err()
	if err != nil {
		if err == redis.Nil {
			return false, nil
		}
		return false, errors.Errorf(logHead+"rds.Expire err:%s", err)
	}
	return true, nil
}
