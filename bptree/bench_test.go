package bptree

import (
	"testing"
)

func Benchmark_Set(b *testing.B) {
	bpt := NewBPTree(1000)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		bpt.Set(int64(i), i)
	}

}

func Benchmark_Get(b *testing.B) {
	bpt := NewBPTree(1000)

	for i := 0; i < 1000000; i++ {
		bpt.Set(int64(i), i)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		bpt.Get(int64(i))
	}

}

func BenchmarkBPTree_Remove(b *testing.B) {
	bpt := NewBPTree(1000)

	for i := 0; i < 1000000; i++ {
		bpt.Set(int64(i), i)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		bpt.Remove(int64(i))
	}

}
