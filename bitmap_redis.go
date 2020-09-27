package bloom

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
)

type RedisConfig struct {
	Address           string
	BitmapKey         string
	RemoveKeyIfExists bool
}

// NewMemoryBitmapSet create bitmap base on memory
func NewRedisBitmapSet(c RedisConfig) BitmapSet {
	return &bitmapSetRedis{
		config: c,
	}
}

// bitmapSetRedis bit map in redis
type bitmapSetRedis struct {
	conn   redis.Conn
	config RedisConfig
}

func (b *bitmapSetRedis) Init(m uint) error {
	if len(b.config.BitmapKey) == 0 {
		return fmt.Errorf("bitmap key is not specific")
	}
	conn, err := redis.Dial("tcp", b.config.Address)
	if err != nil {
		return err
	}

	b.conn = conn

	exists, err := redis.Bool(b.conn.Do("EXISTS", b.config.BitmapKey))
	if err != nil {
		return err
	}
	if exists && !b.config.RemoveKeyIfExists {
		return fmt.Errorf("key already exists")
	}
	if exists && b.config.RemoveKeyIfExists {
		_, err = b.conn.Do("DEL", b.config.BitmapKey)
		if err != nil {
			return err
		}
	}
	return nil

}

func (b *bitmapSetRedis) Set(bits []uint) error {
	err := b.conn.Send("MULTI")
	if err != nil {
		return err
	}
	for _, bit := range bits {
		err = b.conn.Send("SETBIT", b.config.BitmapKey, bit, 1)
		if err != nil {
			return err
		}
	}
	_, err = b.conn.Do("EXEC")
	return err
}

func (b *bitmapSetRedis) Test(bits []uint) (bool, error) {
	err := b.conn.Send("MULTI")
	if err != nil {
		return false, err
	}
	for _, bit := range bits {
		err = b.conn.Send("GETBIT", b.config.BitmapKey, bit)
		if err != nil {
			return false, err
		}
	}
	results, err := redis.Ints(b.conn.Do("EXEC"))
	if err != nil {
		return false, err
	}
	for _, result := range results {
		if result == 0 {
			return false, nil
		}
	}
	return true, nil
}

func (b *bitmapSetRedis) Close() {
	if b.conn != nil {
		_ = b.conn.Close()
	}
}
