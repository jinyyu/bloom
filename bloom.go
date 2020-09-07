package bloom

import "github.com/spaolacci/murmur3"

// BitmapSet interface
type BitmapSet interface {
	// Init init bitmap, m is number of bits in filter
	Init(m uint) error

	// Set each bit in bits, to 1
	Set(bits []uint) error

	// Test whether bit i is set.
	Test(i uint) (bool, error)

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

// baseHashes returns the four hash values of data that are used to create k
// hashes
func baseHashes(data []byte) [4]uint64 {
	a1 := []byte{1} // to grab another bit of data
	hasher := murmur3.New128()
	_, _ = hasher.Write(data) // #nosec
	v1, v2 := hasher.Sum128()
	_, _ = hasher.Write(a1) // #nosec
	v3, v4 := hasher.Sum128()
	return [4]uint64{
		v1, v2, v3, v4,
	}
}

// location returns the ith hashed location using the four base hash values
func (b *Bloom) location(h [4]uint64, i uint) uint {
	ii := uint64(i)
	v := h[ii%2] + ii*h[2+(((ii+(ii%2))%4)/2)]
	return uint(v % uint64(b.m))
}

// Add data to the Bloom Filter
func (b *Bloom) Add(data []byte) error {
	h := baseHashes(data)
	locations := make([]uint, b.k)
	for i := uint(0); i < b.k; i++ {
		l := b.location(h, i)
		locations[i] = l
	}
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
	h := baseHashes(data)
	for i := uint(0); i < b.k; i++ {
		l := b.location(h, i)
		ok, err := b.bms.Test(l)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}

// TestString returns true if the string is in the BloomFilter, false otherwise.
// If true, the result might be a false positive. If false, the data
// is definitely not in the set.
func (b *Bloom) TestString(data string) (bool, error) {
	return b.Test([]byte(data))
}
