package telemetry

import (
	"github.com/wyw14/cry-98/internal/model"
)

const (
	// StreamJournalCapacity bounds how many recent vessel changes are retained
	// for cursor-based reconnects. Cursors older than the retained window are
	// rejected and the client must fetch a fresh snapshot.
	StreamJournalCapacity = 1024
	// StreamSubscriberCapacity bounds how many changes may queue for one client.
	// A client that falls this far behind is flagged lagged and asked to resync
	// instead of blocking probe ingest or growing memory.
	StreamSubscriberCapacity = 256
)

// Change is one vessel update in the stream journal. Seq is a monotonically
// increasing cursor assigned under the snapshot lock, so a snapshot plus its
// cursor is always a consistent starting point.
type Change struct {
	Seq    uint64
	Vessel model.Vessel
}

// StreamSubscription delivers journal changes to a single client. If the
// client falls behind, Lagged is closed and no further events are delivered;
// the client must resync from a fresh snapshot.
type StreamSubscription struct {
	id     uint64
	events chan Change
	lagged chan struct{}
	store  *SnapshotStore
}

func (s *StreamSubscription) Events() <-chan Change { return s.events }

func (s *StreamSubscription) Lagged() <-chan struct{} { return s.lagged }

// Close releases the subscription. It is idempotent and safe to call once the
// client goes away.
func (s *StreamSubscription) Close() {
	s.store.unsubscribe(s.id)
}

type streamSubscriber struct {
	events chan Change
	lagged chan struct{}
}

// SnapshotCursor returns a consistent snapshot together with the cursor a
// client should pass on reconnect to resume after it.
func (s *SnapshotStore) SnapshotCursor() (Snapshot, uint64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	copyMap := make(map[string]model.Vessel, len(s.vessels))
	for id, vessel := range s.vessels {
		copyMap[id] = vessel
	}
	return Snapshot{Vessels: copyMap, UpdatedAt: s.updated}, s.seq
}

// Subscribe registers a client after the given cursor and returns the changes
// it missed. ok is false when the cursor is in the future or has aged out of
// the retained journal; the client must then reconnect without a cursor to
// fetch a fresh snapshot.
func (s *SnapshotStore) Subscribe(afterSeq uint64) (replay []Change, sub *StreamSubscription, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if afterSeq > s.seq {
		return nil, nil, false
	}
	if s.seq-afterSeq > uint64(len(s.journal)) {
		return nil, nil, false
	}
	if s.seq > afterSeq {
		replay = make([]Change, 0, s.seq-afterSeq)
		for _, change := range s.journal {
			if change.Seq > afterSeq {
				replay = append(replay, change)
			}
		}
	}
	s.nextSubscriber++
	sub = &StreamSubscription{
		id:     s.nextSubscriber,
		events: make(chan Change, StreamSubscriberCapacity),
		lagged: make(chan struct{}),
		store:  s,
	}
	s.subscribers[sub.id] = &streamSubscriber{events: sub.events, lagged: sub.lagged}
	return replay, sub, true
}

func (s *SnapshotStore) unsubscribe(id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.subscribers, id)
}

// publish appends the change to the journal and fans it out to subscribers
// without ever blocking: a full subscriber buffer marks the client lagged.
// Callers must hold s.mu.
func (s *SnapshotStore) publish(vessel model.Vessel) {
	s.seq++
	change := Change{Seq: s.seq, Vessel: vessel}
	s.journal = append(s.journal, change)
	if len(s.journal) > StreamJournalCapacity {
		kept := make([]Change, StreamJournalCapacity)
		copy(kept, s.journal[len(s.journal)-StreamJournalCapacity:])
		s.journal = kept
	}
	for id, sub := range s.subscribers {
		select {
		case sub.events <- change:
		default:
			close(sub.lagged)
			delete(s.subscribers, id)
		}
	}
}
