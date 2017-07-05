package base

import "testing"

func Benchmark_SillyName(b *testing.B) {
	for i := 0; i < b.N; i++ {
		GetSillyName()
	}
}
