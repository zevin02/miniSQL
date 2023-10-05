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

func BenchmarkBPTree_GetParallel(b *testing.B) {
	// 创建并初始化BPTree
	bpt := NewBPTree(1000)
	for i := 0; i < 1000000; i++ {
		bpt.Set(int64(i), i)
	}

	// 定义并发测试的子任务数量
	numWorkers := 4

	// 创建结果通道
	results := make(chan int, numWorkers)

	// 启动多个goroutine执行测试用例
	for i := 0; i < numWorkers; i++ {
		go func() {
			for j := 0; j < b.N/numWorkers; j++ {
				bpt.Get(int64(j))
			}
			results <- 1
		}()
	}

	// 等待所有子任务完成
	for i := 0; i < numWorkers; i++ {
		<-results
	}
}
