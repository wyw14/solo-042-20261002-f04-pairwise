package telemetry

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wyw14/cry-98/internal/model"
)

// 游标（cursor）是单调递增的罐体变更序号。初次连接拿到快照及其基线序号，
// 之后每条增量都带新序号；断线重连时带上最后收到的序号即可只补缺失部分。
var (
	// ErrCursorExpired 表示游标落在 broker 保留窗口之外（或来自未知的未来序号），
	// 调用方必须放弃增量追补、重新获取完整快照。
	ErrCursorExpired = errors.New("telemetry stream cursor expired; resnapshot required")
	// ErrStreamLimit 表示在线订阅者已达上限，拒绝新连接以保护内存。
	ErrStreamLimit = errors.New("telemetry stream subscriber limit reached")
)

const (
	// DefaultHistoryDepth 是为断线重连保留的最近变更条数（环形缓冲）。
	DefaultHistoryDepth = 512
	// DefaultSubscriberBuffer 是每个订阅者的待发送通道容量。
	DefaultSubscriberBuffer = 256
	// DefaultMaxSubscribers 是并发流订阅者上限。
	DefaultMaxSubscribers = 256
)

// VesselEvent 是一次罐体变更及其全局序号。
type VesselEvent struct {
	Seq    uint64       `json:"seq"`
	Vessel model.Vessel `json:"vessel"`
}

// Snapshot 是某个序号下的全部罐体一致快照。
type Snapshot struct {
	Vessels   map[string]model.Vessel `json:"vessels"`
	UpdatedAt time.Time               `json:"updated_at"`
	// Seq 是快照的基线序号：快照反映了序号 Seq（含）之前的全部变更。
	Seq uint64 `json:"seq"`
}

// VesselBrokerConfig 配置 VesselBroker；零值字段会被默认值填充。
type VesselBrokerConfig struct {
	HistoryDepth     int
	SubscriberBuffer int
	MaxSubscribers   int
}

// VesselBroker 在内存中维护罐体最新状态、有界变更历史和流订阅者。
//
// 发布路径（探头入站）绝不阻塞：向订阅者投递使用非阻塞发送，
// 跟不上的慢订阅者会被标记过期并摘除，由其自行重新取快照。
type VesselBroker struct {
	depth  int
	buffer int
	maxSub int

	mu          sync.RWMutex
	vessels     map[string]model.Vessel
	updated     time.Time
	lastSeq     uint64
	ring        []VesselEvent
	ringHead    int // 环形中最旧事件的下标
	ringSize    int
	subscribers map[*VesselSubscription]struct{}
}

// VesselSubscription 是一条罐体增量流的订阅句柄。
type VesselSubscription struct {
	ch         chan VesselEvent
	overflowed atomic.Bool

	closeOnce sync.Once
	closed    chan struct{}
	detach    func(*VesselSubscription)
}

// Events 返回增量通道。通道关闭表示订阅结束；若 Expired 为 true，
// 说明本订阅因追不上而被 broker 摘除，消费方必须重新获取完整快照。
func (s *VesselSubscription) Events() <-chan VesselEvent { return s.ch }

// Expired 报告订阅是否因慢消费而被强制结束（错过变更，需要重新快照）。
func (s *VesselSubscription) Expired() bool { return s.overflowed.Load() }

// Done 在订阅被关闭后关闭。
func (s *VesselSubscription) Done() <-chan struct{} { return s.closed }

// Close 注销订阅。幂等；断开 HTTP 连接时必须调用，以及时释放 broker 上的槽位。
func (s *VesselSubscription) Close() {
	s.closeOnce.Do(func() {
		close(s.closed)
		if s.detach != nil {
			s.detach(s)
		}
	})
}

// NewVesselBroker 创建一个空 broker。
func NewVesselBroker(config VesselBrokerConfig) *VesselBroker {
	if config.HistoryDepth <= 0 {
		config.HistoryDepth = DefaultHistoryDepth
	}
	if config.SubscriberBuffer <= 0 {
		config.SubscriberBuffer = DefaultSubscriberBuffer
	}
	if config.MaxSubscribers <= 0 {
		config.MaxSubscribers = DefaultMaxSubscribers
	}
	return &VesselBroker{
		depth:       config.HistoryDepth,
		buffer:      config.SubscriberBuffer,
		maxSub:      config.MaxSubscribers,
		vessels:     make(map[string]model.Vessel),
		ring:        make([]VesselEvent, config.HistoryDepth),
		subscribers: make(map[*VesselSubscription]struct{}),
	}
}

// Publish 记录一次罐体变更并把它投递给所有在线订阅者。
// 非阻塞：慢订阅者投递失败时立即被摘除，不影响其它订阅者和发布方。
func (b *VesselBroker) Publish(vessel model.Vessel) VesselEvent {
	b.mu.Lock()
	event := VesselEvent{Seq: b.lastSeq + 1, Vessel: vessel}
	b.lastSeq = event.Seq
	b.vessels[vessel.ID] = vessel
	b.updated = vessel.UpdatedAt
	b.appendRing(event)
	for sub := range b.subscribers {
		if !b.deliverLocked(sub, event) {
			b.evictLocked(sub)
		}
	}
	b.mu.Unlock()
	return event
}

// Replace 用给定罐体整体替换状态并重置序号基线，用于启动/恢复时装载基线。
// 不应在已有在线订阅者时使用。
func (b *VesselBroker) Replace(vessels []model.Vessel, now time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.vessels = make(map[string]model.Vessel, len(vessels))
	for _, vessel := range vessels {
		b.vessels[vessel.ID] = vessel
	}
	b.updated = now.UTC()
	b.lastSeq = 0
	b.ringHead = 0
	b.ringSize = 0
}

// Open 建立一条新订阅并返回当前一致快照（基线序号 = 快照中的 Seq）。
// 订阅在同一临界区注册，因此快照之后的任何变更都不会漏投到通道里。
func (b *VesselBroker) Open() (*VesselSubscription, Snapshot, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.subscribers) >= b.maxSub {
		return nil, Snapshot{}, ErrStreamLimit
	}
	sub := b.addSubscriberLocked()
	return sub, b.snapshotLocked(), nil
}

// Resume 在游标 after 之后恢复一条订阅：返回 (after, lastSeq] 之间仍在保留
// 窗口内的变更，以及实时订阅通道（订阅已在同一临界区注册，追补段与实时
// 通道严格不重叠、不漏事件）。游标过期时返回 ErrCursorExpired。
func (b *VesselBroker) Resume(after uint64) (*VesselSubscription, []VesselEvent, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	// 可恢复的最小游标 = 最旧保留事件的前一个序号（此时追补从最旧事件开始）。
	minCursor := uint64(0)
	if b.ringSize > 0 {
		minCursor = b.lastSeq - uint64(b.ringSize)
	}
	if after > b.lastSeq || after < minCursor {
		return nil, nil, ErrCursorExpired
	}
	if len(b.subscribers) >= b.maxSub {
		return nil, nil, ErrStreamLimit
	}
	sub := b.addSubscriberLocked()
	return sub, b.backlogLocked(after), nil
}

// StableSnapshot 返回当前状态的一致快照副本。
func (b *VesselBroker) StableSnapshot() Snapshot {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.snapshotLocked()
}

// LastUpdated 返回最近一次变更的时间。
func (b *VesselBroker) LastUpdated() time.Time {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.updated
}

// SubscriberCount 返回当前在线订阅者数量（主要用于测试/可观测性）。
func (b *VesselBroker) SubscriberCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.subscribers)
}

func (b *VesselBroker) addSubscriberLocked() *VesselSubscription {
	sub := &VesselSubscription{
		ch:     make(chan VesselEvent, b.buffer),
		closed: make(chan struct{}),
	}
	sub.detach = b.detach
	b.subscribers[sub] = struct{}{}
	return sub
}

// detach 在订阅者 Close 时被调用（broker 锁外触发，内部自行加锁）。
func (b *VesselBroker) detach(sub *VesselSubscription) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.subscribers[sub]; !ok {
		return
	}
	delete(b.subscribers, sub)
	close(sub.ch)
}

// evictLocked 摘除一个追不上的慢订阅者：标记过期后关闭通道，消费方据此
// 明白自己错过了变更、必须重新取快照。调用方持有 b.mu。
func (b *VesselBroker) evictLocked(sub *VesselSubscription) {
	sub.overflowed.Store(true)
	delete(b.subscribers, sub)
	close(sub.ch)
}

// deliverLocked 非阻塞投递；false 表示订阅者缓冲区已满。调用方持有 b.mu。
func (b *VesselBroker) deliverLocked(sub *VesselSubscription, event VesselEvent) bool {
	select {
	case sub.ch <- event:
		return true
	default:
		return false
	}
}

func (b *VesselBroker) appendRing(event VesselEvent) {
	if b.ringSize < b.depth {
		b.ring[(b.ringHead+b.ringSize)%b.depth] = event
		b.ringSize++
		return
	}
	b.ring[b.ringHead] = event
	b.ringHead = (b.ringHead + 1) % b.depth
}

// backlogLocked 按序复制 (after, lastSeq] 范围内的历史事件。
func (b *VesselBroker) backlogLocked(after uint64) []VesselEvent {
	if b.ringSize == 0 || after >= b.lastSeq {
		return []VesselEvent{}
	}
	count := int(b.lastSeq - after)
	if count > b.ringSize {
		count = b.ringSize
	}
	events := make([]VesselEvent, 0, count)
	// 序号 after+1 在环形中的位置。
	oldest := b.lastSeq - uint64(b.ringSize) + 1
	start := (b.ringHead + int((after+1)-oldest)) % b.depth
	for i := 0; i < count; i++ {
		events = append(events, b.ring[(start+i)%b.depth])
	}
	return events
}

func (b *VesselBroker) snapshotLocked() Snapshot {
	copyMap := make(map[string]model.Vessel, len(b.vessels))
	for id, vessel := range b.vessels {
		copyMap[id] = vessel
	}
	return Snapshot{Vessels: copyMap, UpdatedAt: b.updated, Seq: b.lastSeq}
}
