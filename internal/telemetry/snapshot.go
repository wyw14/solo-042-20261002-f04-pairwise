package telemetry

import (
	"sort"
	"sync"
	"time"

	"github.com/wyw14/cry-98/internal/model"
)

type Snapshot struct {
	Vessels   map[string]model.Vessel `json:"vessels"`
	UpdatedAt time.Time               `json:"updated_at"`
}

type SnapshotStore struct {
	mu             sync.RWMutex
	vessels        map[string]model.Vessel
	updated        time.Time
	seq            uint64
	journal        []Change
	subscribers    map[uint64]*streamSubscriber
	nextSubscriber uint64
}

func NewSnapshotStore() *SnapshotStore {
	return &SnapshotStore{vessels: make(map[string]model.Vessel), subscribers: make(map[uint64]*streamSubscriber)}
}

func (s *SnapshotStore) Apply(vessel model.Vessel) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.vessels[vessel.ID] = vessel
	s.updated = vessel.UpdatedAt
	s.publish(vessel)
}

func (s *SnapshotStore) Get(id string) (model.Vessel, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	vessel, ok := s.vessels[id]
	return vessel, ok
}

func (s *SnapshotStore) StableSnapshot() Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	copyMap := make(map[string]model.Vessel, len(s.vessels))
	for id, vessel := range s.vessels {
		copyMap[id] = vessel
	}
	return Snapshot{Vessels: copyMap, UpdatedAt: s.updated}
}

func (s *SnapshotStore) List() []model.Vessel {
	snapshot := s.StableSnapshot()
	result := make([]model.Vessel, 0, len(snapshot.Vessels))
	for _, vessel := range snapshot.Vessels {
		result = append(result, vessel)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}

func (s *SnapshotStore) Replace(vessels []model.Vessel, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.vessels = make(map[string]model.Vessel, len(vessels))
	for _, vessel := range vessels {
		s.vessels[vessel.ID] = vessel
	}
	s.updated = now.UTC()
	// A wholesale replace cannot be expressed as deltas: invalidate retained
	// history so older cursors expire, and push live subscribers to resync.
	s.seq++
	s.journal = nil
	for id, sub := range s.subscribers {
		close(sub.lagged)
		delete(s.subscribers, id)
	}
}
