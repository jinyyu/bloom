package bloom

import (
	"github.com/spaolacci/murmur3"
)

// BitmapSet interface
type BitmapSet interface {
	// Init init bitmap, m is number of bits in filter
	Init(m uint) error

	// Set each bit in bits, to 1
	Set(bits []uint) error

	// Test whether bits is set.
	Test(bits []uint) (bool, error)

	// Close closes the connection.
	Close()
}

// Bloom bloom filter,
type Bloom struct {
	m   uint      //number of bits in filter
	k   uint      //number of hash functions
	bms BitmapSet //bitmap implement
}

// New creates a new Bloom filter with m bits and k hashing functions
func NewBloomFilter(m uint, k uint, bms BitmapSet) (*Bloom, error) {
	b := Bloom{
		m:   m,
		k:   k,
		bms: bms,
	}
	err := b.bms.Init(m)
	if err != nil {
		return nil, err
	}
	return &b, nil
}

func computeMurmurHash(data []byte, seed uint32) (hashValues [2]uint) {
	h := murmur3.New128WithSeed(seed)
	_, _ = h.Write(data)
	v1, v2 := h.Sum128()
	hashValues[0] = uint(v1)
	hashValues[1] = uint(v2)
	return hashValues
}

// location returns the ith hashed location using the four base hash values
func (b *Bloom) location(data []byte) []uint {
	locations := make([]uint, b.k, b.k)
	index := uint(0)
	for seed := uint32(0); ; seed += 1 {
		//计算一组hash
		hashValues := computeMurmurHash(data, seed)
		for _, hashValue := range hashValues {
			//把这组hash填入locations数组
			locations[index] = hashValue % b.m
			index += 1
			if index == b.k {
				//填满了，返回
				return locations
			}
		}
	}
}

// Add data to the Bloom Filter
func (b *Bloom) Add(data []byte) error {
	locations := b.location(data)
	return b.bms.Set(locations)
}

// AddString to the Bloom Filter
func (b *Bloom) AddString(data string) error {
	return b.Add([]byte(data))
}

// Test returns true if the data is in the Bloom, false otherwise.
// If true, the result might be a false positive. If false, the data
// is definitely not in the set.
func (b *Bloom) Test(data []byte) (bool, error) {
	locations := b.location(data)
	return b.bms.Test(locations)
}

// TestString returns true if the string is in the BloomFilter, false otherwise.
// If true, the result might be a false positive. If false, the data
// is definitely not in the set.
func (b *Bloom) TestString(data string) (bool, error) {
	return b.Test([]byte(data))
}
