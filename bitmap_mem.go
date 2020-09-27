package bloom

import "github.com/willf/bitset"

// bitmapSetMem bit map in memory
type bitmapSetMem struct {
	b *bitset.BitSet
}

func (b *bitmapSetMem) Init(m uint) error {
	b.b = bitset.New(m)
	return nil
}

func (b *bitmapSetMem) Set(bits []uint) error {
	for _, i := range bits {
		b.b.Set(i)
	}
	return nil
}

func (b *bitmapSetMem) Test(bits []uint) (bool, error) {
	for _, i := range bits {
		if !b.b.Test(i) {
			return false, nil
		}
	}
	return true, nil
}

func (b *bitmapSetMem) Close() {
	//nothing to do
}

// NewMemoryBitmapSet create bitmap base on memory
func NewMemoryBitmapSet() BitmapSet {
	return &bitmapSetMem{}
}
