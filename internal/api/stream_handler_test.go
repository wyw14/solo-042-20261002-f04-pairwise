package api

import (
	"bufio"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/wyw14/cry-98/internal/model"
	"github.com/wyw14/cry-98/internal/probe"
	"github.com/wyw14/cry-98/internal/telemetry"
	"github.com/wyw14/cry-98/internal/vessel"
)

type streamEvent struct {
	Type    string          `json:"type"`
	Cursor  uint64          `json:"cursor"`
	Vessels json.RawMessage `json:"vessels"`
	Vessel  json.RawMessage `json:"vessel"`
}

func newStreamTestServer(t *testing.T, heartbeat time.Duration) (*Server, *telemetry.Ingestor, *probe.Registry) {
	t.Helper()
	vessels := vessel.NewRegistry()
	for _, id := range []string{"V-01", "V-02"} {
		item, err := model.NewVessel(id, "Vessel "+id, "north", time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if err := vessels.Register(item); err != nil {
			t.Fatal(err)
		}
	}
	probes := probe.NewRegistry(8)
	probeState, err := model.NewProbe("P-01", model.ProbeLevel, "V-01", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := probes.Register(probeState); err != nil {
		t.Fatal(err)
	}
	snapshots := telemetry.NewSnapshotStore()
	snapshots.Replace(vessels.List(), time.Now())
	ingestor := telemetry.NewIngestor(probes, vessel.NewStateService(vessels, time.Now), snapshots)
	server := NewServer(Dependencies{Telemetry: ingestor, StreamHeartbeat: heartbeat})
	return server, ingestor, probes
}

func readStreamEvent(t *testing.T, reader *bufio.Reader) streamEvent {
	t.Helper()
	line, err := reader.ReadBytes('\n')
	if err != nil {
		t.Fatal(err)
	}
	var event streamEvent
	if err := json.Unmarshal(line, &event); err != nil {
		t.Fatal(err)
	}
	return event
}

func ingestLevel(t *testing.T, ingestor *telemetry.Ingestor, probes *probe.Registry, value float64) {
	t.Helper()
	probeState, ok := probes.Get("P-01")
	if !ok {
		t.Fatal("probe P-01 not registered")
	}
	if err := ingestor.Ingest(probeState.NewSample(value, time.Now())); err != nil {
		t.Fatal(err)
	}
}

func TestStreamVesselsSendsSnapshotThenDeltas(t *testing.T) {
	server, ingestor, probes := newStreamTestServer(t, 50*time.Millisecond)
	upstream := httptest.NewServer(server.Handler())
	defer upstream.Close()

	response, err := http.Get(upstream.URL + "/api/vessels/stream")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", response.StatusCode)
	}
	reader := bufio.NewReader(response.Body)

	first := readStreamEvent(t, reader)
	if first.Type != "snapshot" || first.Cursor == 0 {
		t.Fatalf("expected snapshot event with cursor, got %+v", first)
	}
	var vessels map[string]model.Vessel
	if err := json.Unmarshal(first.Vessels, &vessels); err != nil {
		t.Fatal(err)
	}
	if len(vessels) != 2 {
		t.Fatalf("expected 2 vessels in snapshot, got %d", len(vessels))
	}

	ingestLevel(t, ingestor, probes, 42)
	second := readStreamEvent(t, reader)
	if second.Type != "delta" || second.Cursor <= first.Cursor {
		t.Fatalf("expected delta with advancing cursor, got %+v", second)
	}
	var changed model.Vessel
	if err := json.Unmarshal(second.Vessel, &changed); err != nil {
		t.Fatal(err)
	}
	if changed.ID != "V-01" || changed.LevelMM != 42 {
		t.Fatalf("unexpected delta vessel %+v", changed)
	}

	third := readStreamEvent(t, reader)
	if third.Type != "heartbeat" {
		t.Fatalf("expected heartbeat, got %+v", third)
	}
}

func TestStreamVesselsResumesFromCursor(t *testing.T) {
	server, ingestor, probes := newStreamTestServer(t, time.Hour)
	upstream := httptest.NewServer(server.Handler())
	defer upstream.Close()

	_, cursor := ingestor.SnapshotCursor()
	ingestLevel(t, ingestor, probes, 7)

	request, err := http.NewRequest(http.MethodGet, upstream.URL+"/api/vessels/stream?cursor="+
		strconv.FormatUint(cursor, 10), nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", response.StatusCode)
	}
	reader := bufio.NewReader(response.Body)
	event := readStreamEvent(t, reader)
	if event.Type != "delta" || event.Cursor != cursor+1 {
		t.Fatalf("expected replayed delta at cursor+1, got %+v", event)
	}
}

func TestStreamVesselsRejectsExpiredCursor(t *testing.T) {
	server, ingestor, probes := newStreamTestServer(t, time.Hour)
	upstream := httptest.NewServer(server.Handler())
	defer upstream.Close()

	_, cursor := ingestor.SnapshotCursor()
	for i := 0; i < telemetry.StreamJournalCapacity+1; i++ {
		ingestLevel(t, ingestor, probes, float64(i))
	}

	response, err := http.Get(upstream.URL + "/api/vessels/stream?cursor=" + strconv.FormatUint(cursor, 10))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409 for expired cursor, got %d", response.StatusCode)
	}
	body, _ := io.ReadAll(response.Body)
	if !strings.Contains(string(body), "snapshot") {
		t.Fatalf("expected resync guidance in error body, got %s", body)
	}
}
