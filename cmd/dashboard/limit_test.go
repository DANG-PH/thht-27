package main

import (
	"testing"
	"time"
)

func TestLimiterBlocksBurstThenRefills(t *testing.T) {
	clock := time.Unix(1_800_000_000, 0)
	l := newLimiter(3)
	l.now = func() time.Time { return clock }
	l.last = clock

	for i := 0; i < 3; i++ {
		if !l.allow() {
			t.Fatalf("lần %d phải được phép", i+1)
		}
	}
	if l.allow() {
		t.Fatal("lần thứ 4 trong cùng thời điểm phải bị chặn")
	}
	clock = clock.Add(20 * time.Second) // 3 lần/phút → 20 giây hồi 1 lần
	if !l.allow() {
		t.Fatal("sau 20 giây phải được gửi tiếp")
	}
}

func TestNilLimiterAllowsEverything(t *testing.T) {
	var l *limiter = newLimiter(0)
	for i := 0; i < 1000; i++ {
		if !l.allow() {
			t.Fatal("không giới hạn thì luôn cho phép")
		}
	}
}
