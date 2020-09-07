package bloom

import (
	"fmt"
	"math"
	"testing"
)

func TestMem(t *testing.T) {
	bms := NewMemoryBitmapSet()
	b, _ := NewBloomFilter(1000, 4, bms)

	if ok, _ := b.TestString("abc"); ok {
		t.Errorf("error")
	}
	b.AddString("abc")
	b.AddString("def")
	b.AddString("test")
	if ok, _ := b.TestString("abc"); !ok {
		t.Errorf("error")
	}

	if ok, _ := b.TestString("def"); !ok {
		t.Errorf("error")
	}
	if ok, _ := b.TestString("test"); !ok {
		t.Errorf("error")
	}
}

func TestRedis(t *testing.T) {
	config := RedisConfig{
		Address:           "127.0.0.1:6379",
		BitmapKey:         "bitmap_key",
		RemoveKeyIfExists: true,
	}
	bms := NewRedisBitmapSet(config)
	defer bms.Close()
	b, err := NewBloomFilter(1000, 4, bms)
	if err != nil {
		t.Errorf("%v", err)
	}

	if ok, _ := b.TestString("abc"); ok {
		t.Errorf("error")
	}
	b.AddString("abc")
	b.AddString("def")
	b.AddString("test")
	if ok, _ := b.TestString("abc"); !ok {
		t.Errorf("error")
	}

	if ok, _ := b.TestString("def"); !ok {
		t.Errorf("error")
	}
	if ok, _ := b.TestString("test"); !ok {
		t.Errorf("error")
	}
}

func TestRandRedis(t *testing.T) {
	config := RedisConfig{
		Address:           "127.0.0.1:6379",
		BitmapKey:         "bitmap_key",
		RemoveKeyIfExists: true,
	}
	bms := NewRedisBitmapSet(config)
	defer bms.Close()
	b, err := NewBloomFilter(math.MaxUint32, 7, bms)
	if err != nil {
		t.Errorf("%v", err)
	}

	for i := 0; i <= 10000; i += 1 {
		str := fmt.Sprintf("%d", i)
		err = b.AddString(str)
		if err != nil {
			t.Errorf("%v", err)
		}

		ok, err := b.TestString(str)
		if err != nil {
			t.Errorf("%v", err)
		}
		if !ok {
			t.Errorf("test not ok")
		}
	}
}
