package telemetry

import (
	"testing"
	"time"

	"github.com/wyw14/cry-98/internal/model"
	"github.com/wyw14/cry-98/internal/probe"
	"github.com/wyw14/cry-98/internal/vessel"
)

func TestIngestRejectsObsoleteBindingGeneration(t *testing.T) {
	now := time.Now
	vessels := vessel.NewRegistry()
	vesselState, _ := model.NewVessel("V-01", "Vessel", "north", now())
	if err := vessels.Register(vesselState); err != nil {
		t.Fatal(err)
	}
	probes := probe.NewRegistry(4)
	probeState, _ := model.NewProbe("P-01", model.ProbeLevel, "V-01", now())
	if err := probes.Register(probeState); err != nil {
		t.Fatal(err)
	}
	ingestor := NewIngestor(probes, vessel.NewStateService(vessels, now), NewVesselBroker(VesselBrokerConfig{}))
	stale := probeState.NewSample(50, now())
	if _, err := probe.NewLifecycle(probes, now).Rebind("P-01", "V-02"); err != nil {
		t.Fatal(err)
	}
	if err := ingestor.Ingest(stale); err == nil {
		t.Fatal("obsolete binding sample was accepted")
	}
}
