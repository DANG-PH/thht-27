package main

import (
	"bytes"
	"io"
	"strings"
	"testing"
	"time"

	"nmtht/chude07/internal/evidence"
)

const (
	work   = evidence.ScenarioWork
	pubsub = evidence.ScenarioPubSub
)

func rec(sec int, service, instance, scenario, id string, st evidence.Status, redelivered bool) evidence.Record {
	return evidence.Record{
		Time: time.Unix(int64(1_800_000_000+sec), 0), Service: service, Instance: instance,
		Scenario: scenario, EventID: id, Status: st, Redelivered: redelivered,
	}
}

func published(sec int, scenario, id string) evidence.Record {
	return rec(sec, "academic", "academic", scenario, id, evidence.Published, false)
}

func TestWorkCrashedWorkerStillCountsOnce(t *testing.T) {
	recs := []evidence.Record{
		published(0, work, "ev-aaaa-001"),
		published(0, work, "ev-aaaa-002"),
		rec(1, "finance-worker", "W1", work, "ev-aaaa-001", evidence.Received, false),
		rec(2, "finance-worker", "W1", work, "ev-aaaa-001", evidence.Processed, false),
		rec(1, "finance-worker", "W2", work, "ev-aaaa-002", evidence.Received, false), // W2 chết, chưa ack
		rec(5, "finance-worker", "W1", work, "ev-aaaa-002", evidence.Received, true),
		rec(6, "finance-worker", "W1", work, "ev-aaaa-002", evidence.Processed, true),
	}
	var out bytes.Buffer
	ok, err := reportWork(&out, recs, "")
	if err != nil || !ok {
		t.Fatalf("ok=%v err=%v\n%s", ok, err, out.String())
	}
	if !strings.Contains(out.String(), "Được RabbitMQ giao lại: ev-aaaa-002") {
		t.Fatalf("thiếu dòng giao lại:\n%s", out.String())
	}
}

func TestWorkDuplicateFails(t *testing.T) {
	recs := []evidence.Record{
		published(0, work, "ev-aaaa-001"),
		rec(1, "finance-worker", "W1", work, "ev-aaaa-001", evidence.Processed, false),
		rec(2, "finance-worker", "W2", work, "ev-aaaa-001", evidence.Processed, true),
	}
	if ok, err := reportWork(io.Discard, recs, ""); err != nil || ok {
		t.Fatalf("xử lý 2 lần phải KHÔNG ĐẠT: ok=%v err=%v", ok, err)
	}
}

func TestWorkMissingFails(t *testing.T) {
	recs := []evidence.Record{
		published(0, work, "ev-aaaa-001"),
		published(0, work, "ev-aaaa-002"),
		rec(1, "finance-worker", "W1", work, "ev-aaaa-001", evidence.Processed, false),
	}
	if ok, err := reportWork(io.Discard, recs, ""); err != nil || ok {
		t.Fatalf("thiếu sự kiện phải KHÔNG ĐẠT: ok=%v err=%v", ok, err)
	}
}

func TestWorkPicksLatestRun(t *testing.T) {
	recs := []evidence.Record{
		published(0, work, "ev-old0-001"), // lần cũ, không ai xử lý
		published(10, work, "ev-new0-001"),
		rec(11, "finance-worker", "W1", work, "ev-new0-001", evidence.Processed, false),
	}
	if ok, err := reportWork(io.Discard, recs, ""); err != nil || !ok {
		t.Fatalf("phải chọn lần gửi mới nhất: ok=%v err=%v", ok, err)
	}
}

func TestPubSubEverySubscriberGetsCopy(t *testing.T) {
	recs := []evidence.Record{published(0, pubsub, "ev-bbbb-001"), published(0, pubsub, "ev-bbbb-002")}
	for _, s := range []string{"finance", "notification", "analytics"} {
		for _, id := range []string{"ev-bbbb-001", "ev-bbbb-002"} {
			recs = append(recs, rec(1, s, s, pubsub, id, evidence.Processed, false))
		}
	}
	if ok, err := reportPubSub(io.Discard, recs, ""); err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
}

func TestPubSubMissingCopyFails(t *testing.T) {
	recs := []evidence.Record{
		published(0, pubsub, "ev-bbbb-001"),
		published(0, pubsub, "ev-bbbb-002"),
		rec(1, "finance", "finance", pubsub, "ev-bbbb-001", evidence.Processed, false),
		rec(1, "finance", "finance", pubsub, "ev-bbbb-002", evidence.Processed, false),
		rec(1, "notification", "notification", pubsub, "ev-bbbb-001", evidence.Processed, false),
	}
	if ok, err := reportPubSub(io.Discard, recs, ""); err != nil || ok {
		t.Fatalf("notification thiếu 1 bản sao phải KHÔNG ĐẠT: ok=%v err=%v", ok, err)
	}
}

func TestMalformedNotCountedAsMissing(t *testing.T) {
	bad := published(0, work, "ev-cccc-bad")
	bad.Note = evidence.NoteMalformed
	recs := []evidence.Record{
		published(0, work, "ev-cccc-001"),
		bad,
		rec(1, "finance-worker", "W1", work, "ev-cccc-001", evidence.Processed, false),
		rec(1, "finance-worker", "W2", work, "ev-cccc-bad", evidence.Rejected, false),
	}
	var out bytes.Buffer
	if ok, err := reportWork(&out, recs, ""); err != nil || !ok {
		t.Fatalf("ok=%v err=%v\n%s", ok, err, out.String())
	}
	if !strings.Contains(out.String(), "W2:rejected") {
		t.Fatalf("thiếu kết quả thông điệp sai hợp đồng:\n%s", out.String())
	}
}
