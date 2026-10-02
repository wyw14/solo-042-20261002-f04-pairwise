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
	broker    *VesselBroker
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

func NewIngestor(probes *probe.Registry, vessels *vessel.StateService, broker *VesselBroker) *Ingestor {
	if broker == nil {
		broker = NewVesselBroker(VesselBrokerConfig{})
	}
	return &Ingestor{probes: probes, vessels: vessels, broker: broker}
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
	i.broker.Publish(updated)
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
	return i.broker.StableSnapshot()
}

func (i *Ingestor) LastUpdated() time.Time {
	return i.broker.LastUpdated()
}

// OpenStream 订阅罐体增量流并取得当前一致快照（含基线序号）。
func (i *Ingestor) OpenStream() (*VesselSubscription, Snapshot, error) {
	return i.broker.Open()
}

// ResumeStream 从游标 after 处恢复订阅，返回需要补投的历史变更。
// 游标过期时返回 ErrCursorExpired，调用方应改走 OpenStream 重新取快照。
func (i *Ingestor) ResumeStream(after uint64) (*VesselSubscription, []VesselEvent, error) {
	return i.broker.Resume(after)
}
