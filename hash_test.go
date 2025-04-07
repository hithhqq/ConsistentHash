package zero

import "testing"

const (
	text      = "hello, world!\n"
	md5Digest = "910c8bc73110b0cd1bc5d2bcae782511"
)

func BenchmarkMurmur3(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Hash([]byte(text))
	}
}

func TestHash2(t *testing.T) {
	hash := Hash([]byte(text))
	t.Log(hash)

}
