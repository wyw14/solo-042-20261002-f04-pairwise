package telemetry

import (
	"errors"
	"sync"
	"time"

	"github.com/wyw14/cry-98/internal/model"
	"github.com/wyw14/cry-98/internal/probe"
	"github.com/wyw14/cry-98/internal/vessel"
)

type Ingestor struct {
	probes    *probe.Registry
	vessels   *vessel.StateService
	snapshots *SnapshotStore
	mu        sync.RWMutex
	accepted  uint64
	dropped   uint64
	observers []func(model.Sample)
}

func (i *Ingestor) AddObserver(observer func(model.Sample)) error {
	if observer == nil {
		return errors.New("telemetry observer is required")
	}
	i.mu.Lock()
	i.observers = append(i.observers, observer)
	i.mu.Unlock()
	return nil
}

func NewIngestor(probes *probe.Registry, vessels *vessel.StateService, snapshots *SnapshotStore) *Ingestor {
	return &Ingestor{probes: probes, vessels: vessels, snapshots: snapshots}
}

func (i *Ingestor) Ingest(sample model.Sample) error {
	probeState, ok := i.probes.Get(sample.ProbeID)
	if !ok {
		return errors.New("sample probe is not registered")
	}
	if !probeState.Connected || sample.CalibrationEpoch != probeState.CalibrationEpoch || sample.BindingGeneration != probeState.BindingGeneration {
		i.mu.Lock()
		i.dropped++
		i.mu.Unlock()
		return errors.New("sample belongs to an obsolete probe generation")
	}
	vesselState, ok := i.vessels.Registry().Get(probeState.VesselID)
	if !ok {
		return errors.New("sample vessel is not registered")
	}
	level, pressure, temperature := vesselState.LevelMM, vesselState.PressureKPa, vesselState.TemperatureC
	switch sample.Kind {
	case model.ProbeLevel:
		level = sample.Value
	case model.ProbePressure:
		pressure = sample.Value
	case model.ProbeTemperature:
		temperature = sample.Value
	default:
		return errors.New("unsupported sample kind")
	}
	updated, err := i.vessels.ApplyTelemetry(probeState.VesselID, level, pressure, temperature, sample.ObservedAt)
	if err != nil {
		return err
	}
	i.snapshots.Apply(updated)
	i.mu.Lock()
	i.accepted++
	observers := append([]func(model.Sample){}, i.observers...)
	i.mu.Unlock()
	for _, observer := range observers {
		observer(sample)
	}
	return nil
}

func (i *Ingestor) Stats() (accepted, dropped uint64) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.accepted, i.dropped
}

func (i *Ingestor) Snapshot() Snapshot {
	return i.snapshots.StableSnapshot()
}

// SnapshotCursor returns a consistent snapshot and the cursor to resume from.
func (i *Ingestor) SnapshotCursor() (Snapshot, uint64) {
	return i.snapshots.SnapshotCursor()
}

// Subscribe resumes the change stream after the given cursor. ok is false
// when the cursor expired from the retained journal; the client must fetch a
// fresh snapshot.
func (i *Ingestor) Subscribe(afterSeq uint64) ([]Change, *StreamSubscription, bool) {
	return i.snapshots.Subscribe(afterSeq)
}

func (i *Ingestor) LastUpdated() time.Time {
	return i.snapshots.StableSnapshot().UpdatedAt
}
