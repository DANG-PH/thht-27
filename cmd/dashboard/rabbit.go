package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"
)

// queueInfo là các trường cần dùng trong kết quả GET /api/queues của RabbitMQ Management API.
type queueInfo struct {
	Name      string `json:"name"`
	Ready     int    `json:"messages_ready"`          // đang chờ trong queue
	Unacked   int    `json:"messages_unacknowledged"` // đã giao cho consumer, chưa được ack
	Consumers int    `json:"consumers"`
}

type queuesPayload struct {
	Queues []queueInfo `json:"queues"`
	Error  string      `json:"error,omitempty"`
}

func fetchQueues(ctx context.Context, client *http.Client, base, user, pass string) ([]queueInfo, error) {
	// %2F là vhost mặc định "/" đã mã hóa URL.
	url := strings.TrimRight(base, "/") + "/api/queues/%2F?columns=name,messages_ready,messages_unacknowledged,consumers"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(user, pass)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	var qs []queueInfo
	if err := json.NewDecoder(resp.Body).Decode(&qs); err != nil {
		return nil, err
	}
	sort.Slice(qs, func(i, j int) bool { return qs[i].Name < qs[j].Name })
	return qs, nil
}

// runQueuePoller hỏi số liệu queue mỗi giây và phát cho trình duyệt khi có thay đổi.
func runQueuePoller(ctx context.Context, h *hub, base, user, pass string) {
	client := &http.Client{Timeout: 3 * time.Second}
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		qs, err := fetchQueues(ctx, client, base, user, pass)
		p := queuesPayload{Queues: qs}
		if p.Queues == nil {
			p.Queues = []queueInfo{}
		}
		if err != nil {
			p.Error = "không đọc được RabbitMQ Management API: " + err.Error()
		}
		if b, err := json.Marshal(p); err == nil {
			h.publishQueues(b)
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}
