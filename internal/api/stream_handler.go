package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/wyw14/cry-98/internal/model"
	"github.com/wyw14/cry-98/internal/telemetry"
)

// streamHeartbeatInterval 必须明显短于 http.Server.IdleTimeout（60s），
// 保证空闲连接上也有数据可 flush，客户端/中间代理不会误判断连。
const streamHeartbeatInterval = 15 * time.Second

// streamMessage 是 /api/vessels/stream 的 NDJSON 消息信封。
//
// 消息类型：
//   - snapshot:        连接起点的一致快照，seq 为基线序号
//   - vessel:          单条罐体增量，seq 为该变更序号（客户端据此推进游标）
//   - heartbeat:       空闲心跳，seq 回显最后已发送序号（即客户端游标）
//   - resync_required: 服务端保留窗口/订阅缓冲已覆盖不了客户端状态，
//     必须丢弃本地状态、重新发起一次不带游标的连接取完整快照
type streamMessage struct {
	Type       string         `json:"type"`
	Seq        uint64         `json:"seq,omitempty"`
	At         time.Time      `json:"at,omitempty"`
	SnapshotAt time.Time      `json:"snapshot_at,omitempty"`
	Reason     string         `json:"reason,omitempty"`
	Vessels    []model.Vessel `json:"vessels,omitempty"`
	Vessel     *model.Vessel  `json:"vessel,omitempty"`
}

func (s *Server) streamVessels(writer http.ResponseWriter, request *http.Request) {
	ctx := request.Context()
	now := s.deps.Now

	// ?cursor= 缺省（或为 0）表示全新连接；否则从游标处增量恢复。
	resume := false
	var cursor uint64
	if raw := request.URL.Query().Get("cursor"); raw != "" {
		parsed, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			writeError(writer, http.StatusBadRequest, "cursor must be a non-negative integer")
			return
		}
		if parsed > 0 {
			resume = true
			cursor = parsed
		}
	}

	// 在切换到 200 NDJSON 之前完成订阅建立与游标校验：
	// 游标过期 / 订阅超限时用普通 JSON 错误码表达，客户端不必先解析流。
	var (
		sub     *telemetry.VesselSubscription
		snap    telemetry.Snapshot
		backlog []telemetry.VesselEvent
	)
	if resume {
		subscription, events, err := s.deps.Telemetry.ResumeStream(cursor)
		if err != nil {
			s.writeStreamSetupError(writer, err)
			return
		}
		sub, backlog = subscription, events
	} else {
		subscription, snapshot, err := s.deps.Telemetry.OpenStream()
		if err != nil {
			s.writeStreamSetupError(writer, err)
			return
		}
		sub, snap = subscription, snapshot
	}
	// 任何退出路径（客户端断开、写入失败、被 broker 摘除）都及时释放订阅槽位。
	defer sub.Close()

	writer.Header().Set("Content-Type", "application/x-ndjson")
	writer.Header().Set("Cache-Control", "no-cache")
	writer.Header().Set("X-Accel-Buffering", "no")
	writer.WriteHeader(http.StatusOK)

	flusher, _ := writer.(http.Flusher)
	encoder := json.NewEncoder(writer)
	lastSeq := cursor

	if resume {
		// 追补段是 Resume 时在 broker 临界区内复制的；订阅也在同一临界区注册，
		// 之后的实时变更只会出现在通道中，与追补段不重不漏。
		for i := range backlog {
			if err := encoder.Encode(streamMessage{
				Type:   "vessel",
				Seq:    backlog[i].Seq,
				Vessel: &backlog[i].Vessel,
			}); err != nil {
				return
			}
			lastSeq = backlog[i].Seq
		}
	} else {
		if err := encoder.Encode(streamMessage{
			Type:       "snapshot",
			Seq:        snap.Seq,
			SnapshotAt: snap.UpdatedAt,
			Vessels:    sortedStreamVessels(snap),
		}); err != nil {
			return
		}
		lastSeq = snap.Seq
	}
	if flusher != nil {
		flusher.Flush()
	}

	heartbeat := time.NewTicker(s.streamHeartbeat)
	defer heartbeat.Stop()

	for {
		select {
		case <-ctx.Done():
			// 客户端断开：defer 中的 sub.Close() 立即释放 broker 上的订阅。
			return
		case <-heartbeat.C:
			if err := encoder.Encode(streamMessage{
				Type: "heartbeat",
				Seq:  lastSeq,
				At:   now(),
			}); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
		case event, ok := <-sub.Events():
			if !ok {
				s.endStreamOnExpiry(encoder, flusher, sub, now)
				return
			}
			if err := encoder.Encode(streamMessage{
				Type:   "vessel",
				Seq:    event.Seq,
				Vessel: &event.Vessel,
			}); err != nil {
				return
			}
			lastSeq = event.Seq
			// 顺手排空通道里已堆积的事件再 flush；通道一空立刻回到 select，
			// 任何一步写失败都直接退出（defer 负责释放订阅）。
		drain:
			for {
				select {
				case next, ok := <-sub.Events():
					if !ok {
						s.endStreamOnExpiry(encoder, flusher, sub, now)
						return
					}
					if err := encoder.Encode(streamMessage{
						Type:   "vessel",
						Seq:    next.Seq,
						Vessel: &next.Vessel,
					}); err != nil {
						return
					}
					lastSeq = next.Seq
				default:
					break drain
				}
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
	}
}

// endStreamOnExpiry 在订阅通道被关闭时收尾：仅当慢消费导致过期才下发
// resync_required，明确要求客户端重新取完整快照。
func (s *Server) endStreamOnExpiry(
	encoder *json.Encoder,
	flusher http.Flusher,
	sub *telemetry.VesselSubscription,
	now func() time.Time,
) {
	if sub.Expired() {
		_ = encoder.Encode(streamMessage{
			Type:   "resync_required",
			At:     now(),
			Reason: "stream cursor expired on server; reopen without cursor for a fresh snapshot",
		})
		if flusher != nil {
			flusher.Flush()
		}
	}
}

func (s *Server) writeStreamSetupError(writer http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, telemetry.ErrCursorExpired):
		// 明确要求重新取快照：客户端应以不带 cursor 的新连接重来。
		writeJSON(writer, http.StatusConflict, map[string]string{
			"error":  "cursor expired; history no longer covers it",
			"code":   "resnapshot_required",
			"action": "reconnect without cursor and apply a fresh snapshot",
		})
	case errors.Is(err, telemetry.ErrStreamLimit):
		writer.Header().Set("Retry-After", "5")
		writeJSON(writer, http.StatusServiceUnavailable, map[string]string{
			"error": "too many concurrent vessel streams; retry later",
			"code":  "stream_limit",
		})
	default:
		writeError(writer, http.StatusInternalServerError, err.Error())
	}
}

func sortedStreamVessels(snapshot telemetry.Snapshot) []model.Vessel {
	result := make([]model.Vessel, 0, len(snapshot.Vessels))
	for _, vessel := range snapshot.Vessels {
		result = append(result, vessel)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}
