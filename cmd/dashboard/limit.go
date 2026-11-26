package main

import (
	"sync"
	"time"
)

// limiter giới hạn số lần gửi trên toàn trang (thuật toán token bucket), để trang công khai
// không bị spam làm đầy queue và ổ đĩa. nil nghĩa là không giới hạn.
type limiter struct {
	mu       sync.Mutex
	capacity float64 // số lần gửi dồn liền tối đa
	tokens   float64
	perSec   float64 // tốc độ hồi lại
	last     time.Time
	now      func() time.Time
}

func newLimiter(perMinute int) *limiter {
	if perMinute <= 0 {
		return nil
	}
	return &limiter{
		capacity: float64(perMinute),
		tokens:   float64(perMinute),
		perSec:   float64(perMinute) / 60,
		last:     time.Now(),
		now:      time.Now,
	}
}

func (l *limiter) allow() bool {
	if l == nil {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	l.tokens = min(l.capacity, l.tokens+now.Sub(l.last).Seconds()*l.perSec)
	l.last = now
	if l.tokens < 1 {
		return false
	}
	l.tokens--
	return true
}
