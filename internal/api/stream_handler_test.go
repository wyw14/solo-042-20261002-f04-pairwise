package api

import (
	"bufio"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/wyw14/cry-98/internal/model"
	"github.com/wyw14/cry-98/internal/telemetry"
)

func newStreamTestServer(t *testing.T, config telemetry.VesselBrokerConfig, heartbeat time.Duration) (*httptest.Server, *telemetry.VesselBroker) {
	t.Helper()
	broker := telemetry.NewVesselBroker(config)
	now := time.Unix(1_700_000_000, 0)
	v1, _ := model.NewVessel("V-01", "Vessel one", "north", now)
	v2, _ := model.NewVessel("V-02", "Vessel two", "south", now)
	broker.Replace([]model.Vessel{v1, v2}, now)

	server := NewServer(Dependencies{
		// 流处理器只依赖 Telemetry 与 Now；其余依赖在本路由不被触达。
		Telemetry:       telemetry.NewIngestor(nil, nil, broker),
		Now:             time.Now,
		StreamHeartbeat: heartbeat,
	})
	ts := httptest.NewServer(server.Handler())
	t.Cleanup(ts.Close)
	return ts, broker
}

func streamVessel(id string, value float64) model.Vessel {
	v, _ := model.NewVessel(id, "Vessel "+id, "north", time.Unix(1_700_000_000, 0))
	return v.WithTelemetry(value, 100, 10, time.Unix(1_700_000_100+int64(value), 0))
}

func openStream(t *testing.T, ts *httptest.Server, query string) (*http.Response, *bufio.Reader) {
	t.Helper()
	resp, err := http.Get(ts.URL + "/api/vessels/stream" + query)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	return resp, bufio.NewReader(resp.Body)
}

func decodeStreamLine(t *testing.T, reader *bufio.Reader) map[string]any {
	t.Helper()
	line, err := reader.ReadBytes('\n')
	if err != nil {
		t.Fatalf("read stream line: %v", err)
	}
	var message map[string]any
	if err := json.Unmarshal(line, &message); err != nil {
		t.Fatalf("invalid ndjson %q: %v", line, err)
	}
	return message
}

func TestStreamFreshConnectionSendsSnapshotThenChanges(t *testing.T) {
	ts, broker := newStreamTestServer(t, telemetry.VesselBrokerConfig{}, time.Minute)

	resp, reader := openStream(t, ts, "")
	defer resp.Body.Close()

	snapshot := decodeStreamLine(t, reader)
	if snapshot["type"] != "snapshot" {
		t.Fatalf("first message type = %v, want snapshot", snapshot["type"])
	}
	vessels := snapshot["vessels"].([]any)
	if len(vessels) != 2 {
		t.Fatalf("snapshot vessels = %d, want 2", len(vessels))
	}

	// 后续只收到发生变化的罐体。
	broker.Publish(streamVessel("V-01", 42))
	update := decodeStreamLine(t, reader)
	if update["type"] != "vessel" {
		t.Fatalf("update type = %v, want vessel", update["type"])
	}
	vessel := update["vessel"].(map[string]any)
	if vessel["id"] != "V-01" {
		t.Fatalf("update vessel id = %v, want V-01", vessel["id"])
	}
	if update["seq"].(float64) != 1 {
		t.Fatalf("update seq = %v, want 1", update["seq"])
	}
}

func TestStreamResumeReplaysOnlyChangesAfterCursor(t *testing.T) {
	ts, broker := newStreamTestServer(t, telemetry.VesselBrokerConfig{HistoryDepth: 32}, time.Minute)
	broker.Publish(streamVessel("V-01", 1))
	broker.Publish(streamVessel("V-02", 2))
	broker.Publish(streamVessel("V-01", 3))

	resp, reader := openStream(t, ts, "?cursor=1")
	defer resp.Body.Close()

	first := decodeStreamLine(t, reader)
	if first["type"] != "vessel" || first["seq"].(float64) != 2 {
		t.Fatalf("first replay = %v, want vessel seq 2", first)
	}
	second := decodeStreamLine(t, reader)
	if second["type"] != "vessel" || second["seq"].(float64) != 3 {
		t.Fatalf("second replay = %v, want vessel seq 3", second)
	}
	if vessel := second["vessel"].(map[string]any); vessel["id"] != "V-01" {
		t.Fatalf("second replay vessel = %v, want V-01", vessel["id"])
	}
}

func TestStreamExpiredCursorRequestsResnapshot(t *testing.T) {
	ts, broker := newStreamTestServer(t, telemetry.VesselBrokerConfig{HistoryDepth: 2}, time.Minute)
	for range 10 {
		broker.Publish(streamVessel("V-01", 1))
	}

	resp, err := http.Get(ts.URL + "/api/vessels/stream?cursor=1")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want 409", resp.StatusCode)
	}
	var body map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body["code"] != "resnapshot_required" {
		t.Fatalf("error code = %q, want resnapshot_required", body["code"])
	}
}

func TestStreamInvalidCursorRejected(t *testing.T) {
	ts, _ := newStreamTestServer(t, telemetry.VesselBrokerConfig{}, time.Minute)
	resp, err := http.Get(ts.URL + "/api/vessels/stream?cursor=abc")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestStreamSendsHeartbeatWhenIdle(t *testing.T) {
	ts, _ := newStreamTestServer(t, telemetry.VesselBrokerConfig{}, 20*time.Millisecond)

	resp, reader := openStream(t, ts, "")
	defer resp.Body.Close()

	_ = decodeStreamLine(t, reader) // 快照
	heartbeat := decodeStreamLine(t, reader)
	if heartbeat["type"] != "heartbeat" {
		t.Fatalf("idle message type = %v, want heartbeat", heartbeat["type"])
	}
}

func TestStreamSubscriberLimitReturns503(t *testing.T) {
	ts, _ := newStreamTestServer(t, telemetry.VesselBrokerConfig{MaxSubscribers: 1}, time.Minute)

	first, _ := openStream(t, ts, "")
	defer first.Body.Close()

	resp, err := http.Get(ts.URL + "/api/vessels/stream")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", resp.StatusCode)
	}
	if resp.Header.Get("Retry-After") == "" {
		t.Fatal("expected Retry-After header on 503")
	}
}

func TestStreamDisconnectReleasesSubscription(t *testing.T) {
	ts, broker := newStreamTestServer(t, telemetry.VesselBrokerConfig{MaxSubscribers: 1}, time.Minute)

	resp, _ := openStream(t, ts, "")
	if count := broker.SubscriberCount(); count != 1 {
		t.Fatalf("subscriber count = %d, want 1", count)
	}
	_ = resp.Body.Close()

	deadline := time.Now().Add(time.Second)
	for broker.SubscriberCount() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("subscription not released, count = %d", broker.SubscriberCount())
		}
		time.Sleep(5 * time.Millisecond)
	}

	// 槽位释放后可以立即建立新连接。
	other, _ := openStream(t, ts, "")
	defer other.Body.Close()
}

func TestStreamSlowClientGetsResyncRequired(t *testing.T) {
	ts, broker := newStreamTestServer(t,
		telemetry.VesselBrokerConfig{SubscriberBuffer: 2, HistoryDepth: 64}, time.Minute)

	resp, reader := openStream(t, ts, "")
	_ = decodeStreamLine(t, reader) // 取走快照后停止读取，模拟慢客户端。

	// 大量变更灌满订阅缓冲，broker 应摘除该订阅而不是阻塞发布。
	for i := range 200 {
		broker.Publish(streamVessel("V-01", float64(i)))
	}

	// 恢复读取后，最终必须收到明确的 resync_required。
	deadline := time.Now().Add(2 * time.Second)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			t.Fatalf("stream closed before resync_required: %v", err)
		}
		var message map[string]any
		if err := json.Unmarshal(line, &message); err != nil {
			t.Fatal(err)
		}
		if message["type"] == "resync_required" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("never received resync_required")
		}
	}
	_ = resp.Body.Close()
}
