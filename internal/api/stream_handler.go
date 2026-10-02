package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/wyw14/cry-98/internal/telemetry"
)

// streamVessels serves a continuous NDJSON stream of vessel state. On first
// connect it emits one "snapshot" event carrying a consistent view and a
// cursor; afterwards only "delta" events for changed vessels are sent, plus
// periodic "heartbeat" events. A disconnected client may resume with
// ?cursor=<seq>; if the cursor has aged out of the retained journal the
// request fails with 409 and the client must reconnect without a cursor to
// fetch a fresh snapshot. Clients that cannot keep up receive a final
// "resync" event before the stream is closed, so slow consumers never block
// probe ingest.
func (s *Server) streamVessels(writer http.ResponseWriter, request *http.Request) {
	flusher, ok := writer.(http.Flusher)
	if !ok {
		writeError(writer, http.StatusInternalServerError, "streaming is not supported")
		return
	}

	var replay []telemetry.Change
	var subscription *telemetry.StreamSubscription
	var snapshot *telemetry.Snapshot
	var cursor uint64
	if raw := request.URL.Query().Get("cursor"); raw != "" {
		parsed, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			writeError(writer, http.StatusBadRequest, "cursor must be an unsigned integer")
			return
		}
		cursor = parsed
		missed, sub, resumed := s.deps.Telemetry.Subscribe(cursor)
		if !resumed {
			writeError(writer, http.StatusConflict, "cursor expired; reconnect without a cursor to fetch a fresh snapshot")
			return
		}
		replay, subscription = missed, sub
	} else {
		consistent, at := s.deps.Telemetry.SnapshotCursor()
		cursor = at
		missed, sub, resumed := s.deps.Telemetry.Subscribe(cursor)
		if !resumed {
			writeError(writer, http.StatusConflict, "cursor expired; reconnect without a cursor to fetch a fresh snapshot")
			return
		}
		replay, subscription = missed, sub
		snapshot = &consistent
	}
	defer subscription.Close()

	writer.Header().Set("Content-Type", "application/x-ndjson")
	writer.Header().Set("Cache-Control", "no-cache")
	writer.Header().Set("X-Accel-Buffering", "no")
	writer.WriteHeader(http.StatusOK)

	encoder := json.NewEncoder(writer)
	send := func(event any) bool {
		if err := encoder.Encode(event); err != nil {
			return false
		}
		flusher.Flush()
		return true
	}

	if snapshot != nil {
		if !send(map[string]any{
			"type":        "snapshot",
			"cursor":      cursor,
			"snapshot_at": snapshot.UpdatedAt,
			"vessels":     snapshot.Vessels,
		}) {
			return
		}
	}
	lastCursor := cursor
	for _, change := range replay {
		if !send(streamDeltaEvent(change)) {
			return
		}
		lastCursor = change.Seq
	}

	interval := s.deps.StreamHeartbeat
	if interval <= 0 {
		interval = 15 * time.Second
	}
	heartbeat := time.NewTicker(interval)
	defer heartbeat.Stop()
	for {
		select {
		case <-request.Context().Done():
			return
		case <-subscription.Lagged():
			send(map[string]any{
				"type":   "resync",
				"reason": "client fell behind; reconnect without a cursor to fetch a fresh snapshot",
			})
			return
		case change := <-subscription.Events():
			if !send(streamDeltaEvent(change)) {
				return
			}
			lastCursor = change.Seq
		case <-heartbeat.C:
			if !send(map[string]any{
				"type":   "heartbeat",
				"cursor": lastCursor,
				"at":     s.deps.Now().UTC(),
			}) {
				return
			}
		}
	}
}

func streamDeltaEvent(change telemetry.Change) map[string]any {
	return map[string]any{
		"type":   "delta",
		"cursor": change.Seq,
		"id":     change.Vessel.ID,
		"vessel": change.Vessel,
	}
}
