package telemetry

import (
	"errors"
	"testing"
	"time"

	"github.com/wyw14/cry-98/internal/model"
)

func testVessel(id string, value float64) model.Vessel {
	v, err := model.NewVessel(id, "Vessel "+id, "north", time.Unix(1_700_000_000, 0))
	if err != nil {
		panic(err)
	}
	return v.WithTelemetry(value, 100, 10, time.Unix(1_700_000_000+int64(value), 0))
}

func TestOpenReturnsConsistentSnapshotAndLiveUpdates(t *testing.T) {
	broker := NewVesselBroker(VesselBrokerConfig{})
	broker.Replace([]model.Vessel{testVessel("V-01", 1), testVessel("V-02", 2)}, time.Unix(1, 0))

	sub, snapshot, err := broker.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	if snapshot.Seq != 0 {
		t.Fatalf("baseline seq = %d, want 0", snapshot.Seq)
	}
	if len(snapshot.Vessels) != 2 {
		t.Fatalf("snapshot size = %d, want 2", len(snapshot.Vessels))
	}
	if broker.SubscriberCount() != 1 {
		t.Fatalf("subscriber count = %d, want 1", broker.SubscriberCount())
	}

	// 快照建立之后的变更必须到达订阅者，且只发变化的罐体。
	event := broker.Publish(testVessel("V-01", 11))
	if event.Seq != 1 {
		t.Fatalf("event seq = %d, want 1", event.Seq)
	}
	select {
	case received := <-sub.Events():
		if received.Seq != 1 || received.Vessel.ID != "V-01" || received.Vessel.LevelMM != 11 {
			t.Fatalf("unexpected event: %+v", received)
		}
	case <-time.After(time.Second):
		t.Fatal("live event not delivered")
	}

	snapshotAgain := broker.StableSnapshot()
	if snapshotAgain.Seq != 1 {
		t.Fatalf("snapshot seq = %d, want 1", snapshotAgain.Seq)
	}
	if snapshotAgain.Vessels["V-01"].LevelMM != 11 || snapshotAgain.Vessels["V-02"].LevelMM != 2 {
		t.Fatalf("snapshot state inconsistent: %+v", snapshotAgain.Vessels)
	}
}

func TestResumeReplaysOnlyMissingEvents(t *testing.T) {
	broker := NewVesselBroker(VesselBrokerConfig{HistoryDepth: 8})

	// 没有任何订阅者时发生 1..5 号变更。
	for i := 0; i < 5; i++ {
		broker.Publish(testVessel("V-01", float64(i+1)))
	}

	sub, backlog, err := broker.Resume(2)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	if len(backlog) != 3 {
		t.Fatalf("backlog len = %d, want 3", len(backlog))
	}
	for i, event := range backlog {
		if event.Seq != uint64(3+i) {
			t.Fatalf("backlog[%d].Seq = %d, want %d", i, event.Seq, 3+i)
		}
	}

	// 追补段之后的实时变更继续从通道送达。
	broker.Publish(testVessel("V-01", 6))
	select {
	case event := <-sub.Events():
		if event.Seq != 6 {
			t.Fatalf("live event seq = %d, want 6", event.Seq)
		}
	case <-time.After(time.Second):
		t.Fatal("live event not delivered after resume")
	}
}

func TestResumeAtCurrentSeqGetsNoBacklogAndKeepsStreaming(t *testing.T) {
	broker := NewVesselBroker(VesselBrokerConfig{HistoryDepth: 4})
	broker.Publish(testVessel("V-01", 1))

	sub, backlog, err := broker.Resume(1)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	if len(backlog) != 0 {
		t.Fatalf("backlog len = %d, want 0", len(backlog))
	}
	broker.Publish(testVessel("V-01", 2))
	select {
	case event := <-sub.Events():
		if event.Seq != 2 {
			t.Fatalf("event seq = %d, want 2", event.Seq)
		}
	case <-time.After(time.Second):
		t.Fatal("expected live event")
	}
}

func TestResumeRejectsExpiredAndFutureCursors(t *testing.T) {
	depth := 4
	broker := NewVesselBroker(VesselBrokerConfig{HistoryDepth: depth})
	for i := 0; i < depth+3; i++ {
		broker.Publish(testVessel("V-01", float64(i)))
	}

	// 最旧保留事件为 seq=4，其前一个序号 3 是最小合法游标；游标 2 已越界。
	if _, _, err := broker.Resume(2); !errors.Is(err, ErrCursorExpired) {
		t.Fatalf("expired cursor err = %v, want ErrCursorExpired", err)
	}
	// 未来游标也不能接受，避免伪造游标绕过重新快照。
	if _, _, err := broker.Resume(999); !errors.Is(err, ErrCursorExpired) {
		t.Fatalf("future cursor err = %v, want ErrCursorExpired", err)
	}
	// 边界游标仍可恢复，追补从最旧保留事件开始。
	sub, backlog, err := broker.Resume(3)
	if err != nil {
		t.Fatalf("oldest-valid cursor rejected: %v", err)
	}
	defer sub.Close()
	if len(backlog) != depth {
		t.Fatalf("backlog len = %d, want %d", len(backlog), depth)
	}
}

func TestSlowSubscriberDoesNotBlockPublishAndIsEvicted(t *testing.T) {
	broker := NewVesselBroker(VesselBrokerConfig{HistoryDepth: 128, SubscriberBuffer: 2})
	sub, _, err := broker.Open()
	if err != nil {
		t.Fatal(err)
	}

	// 慢客户端：一个订阅者不读通道；发布必须继续、不能阻塞探头入站。
	for i := 0; i < 50; i++ {
		done := make(chan struct{})
		go func() { broker.Publish(testVessel("V-01", float64(i))); close(done) }()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatalf("publish %d blocked by slow subscriber", i)
		}
	}

	// 慢订阅者应被摘除并标记过期：排空缓冲后通道关闭。
	deadline := time.After(time.Second)
	for {
		select {
		case _, ok := <-sub.Events():
			if !ok {
				goto evicted
			}
		case <-deadline:
			t.Fatal("evicted subscriber channel was not closed")
		}
	}
evicted:
	if !sub.Expired() {
		t.Fatal("evicted subscriber should report Expired")
	}
	if count := broker.SubscriberCount(); count != 0 {
		t.Fatalf("subscriber count after eviction = %d, want 0", count)
	}
}

func TestEvictingSlowSubscriberDoesNotAffectFuturePublishes(t *testing.T) {
	broker := NewVesselBroker(VesselBrokerConfig{SubscriberBuffer: 2})
	slow, _, err := broker.Open()
	if err != nil {
		t.Fatal(err)
	}

	// 突发超过慢订阅者缓冲，它被摘除。
	for i := 0; i < 5; i++ {
		broker.Publish(testVessel("V-01", float64(i)))
	}
	for range slow.Events() {
	}
	if !slow.Expired() {
		t.Fatal("slow subscriber should be expired")
	}

	// 摘除后新订阅者能正常拿到快照和后续实时流，发布路径未受影响。
	fresh, snapshot, err := broker.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	if snapshot.Seq != 5 {
		t.Fatalf("snapshot seq = %d, want 5", snapshot.Seq)
	}
	broker.Publish(testVessel("V-01", 99))
	select {
	case event := <-fresh.Events():
		if event.Seq != 6 || event.Vessel.LevelMM != 99 {
			t.Fatalf("unexpected event after eviction: %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("live event not delivered after slow subscriber eviction")
	}
}

func TestCloseReleasesSubscriberSlot(t *testing.T) {
	broker := NewVesselBroker(VesselBrokerConfig{MaxSubscribers: 1})
	sub, _, err := broker.Open()
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := broker.Open(); !errors.Is(err, ErrStreamLimit) {
		t.Fatalf("expected ErrStreamLimit, got %v", err)
	}
	sub.Close()
	// 关闭幂等，且槽位立即释放。
	sub.Close()
	other, _, err := broker.Open()
	if err != nil {
		t.Fatalf("slot not released after close: %v", err)
	}
	defer other.Close()

	// 关闭后投递到已注销订阅者不会 panic，事件通道也已关闭。
	broker.Publish(testVessel("V-01", 1))
	if _, ok := <-sub.Events(); ok {
		t.Fatal("closed subscriber channel should be drained/closed")
	}
}

func TestSubscriberLimitAppliesToResume(t *testing.T) {
	broker := NewVesselBroker(VesselBrokerConfig{MaxSubscribers: 1, HistoryDepth: 4})
	broker.Publish(testVessel("V-01", 1))
	sub, _, err := broker.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	if _, _, err := broker.Resume(0); !errors.Is(err, ErrStreamLimit) {
		t.Fatalf("resume limit err = %v, want ErrStreamLimit", err)
	}
}
