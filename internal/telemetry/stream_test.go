package telemetry

import (
	"testing"
	"time"

	"github.com/wyw14/cry-98/internal/model"
)

func streamTestVessel(t *testing.T, id string, level float64) model.Vessel {
	t.Helper()
	vessel, err := model.NewVessel(id, "Vessel "+id, "north", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	vessel.LevelMM = level
	vessel.UpdatedAt = time.Now().UTC()
	return vessel
}

func TestSubscribeReplaysMissedChanges(t *testing.T) {
	store := NewSnapshotStore()
	store.Apply(streamTestVessel(t, "V-01", 1))
	_, cursor := store.SnapshotCursor()
	store.Apply(streamTestVessel(t, "V-01", 2))
	store.Apply(streamTestVessel(t, "V-02", 3))

	replay, sub, ok := store.Subscribe(cursor)
	if !ok {
		t.Fatal("subscribe from recent cursor was rejected")
	}
	defer sub.Close()
	if len(replay) != 2 || replay[0].Seq != cursor+1 || replay[1].Seq != cursor+2 {
		t.Fatalf("unexpected replay %+v", replay)
	}

	store.Apply(streamTestVessel(t, "V-01", 4))
	select {
	case change := <-sub.Events():
		if change.Seq != cursor+3 || change.Vessel.LevelMM != 4 {
			t.Fatalf("unexpected live change %+v", change)
		}
	case <-time.After(time.Second):
		t.Fatal("live change was not delivered to subscriber")
	}
}

func TestSubscribeRejectsExpiredAndFutureCursors(t *testing.T) {
	store := NewSnapshotStore()
	store.Apply(streamTestVessel(t, "V-01", 1))
	_, cursor := store.SnapshotCursor()
	for i := 0; i < StreamJournalCapacity+1; i++ {
		store.Apply(streamTestVessel(t, "V-01", float64(i)))
	}
	if _, _, ok := store.Subscribe(cursor); ok {
		t.Fatal("expired cursor was accepted")
	}
	_, future := store.SnapshotCursor()
	if _, _, ok := store.Subscribe(future + 1); ok {
		t.Fatal("future cursor was accepted")
	}
	if _, sub, ok := store.Subscribe(future); !ok {
		t.Fatal("current cursor was rejected")
	} else {
		sub.Close()
	}
}

func TestSlowSubscriberIsLaggedWithoutBlockingApply(t *testing.T) {
	store := NewSnapshotStore()
	store.Apply(streamTestVessel(t, "V-01", 1))
	_, cursor := store.SnapshotCursor()
	_, sub, ok := store.Subscribe(cursor)
	if !ok {
		t.Fatal("subscribe was rejected")
	}
	defer sub.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < StreamSubscriberCapacity+1; i++ {
			store.Apply(streamTestVessel(t, "V-01", float64(i)))
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("apply blocked on a slow subscriber")
	}
	select {
	case <-sub.Lagged():
	default:
		t.Fatal("slow subscriber was not flagged lagged")
	}
	if got := len(store.subscribers); got != 0 {
		t.Fatalf("lagged subscriber was not released, %d remain", got)
	}
}

func TestUnsubscribeReleasesSubscriber(t *testing.T) {
	store := NewSnapshotStore()
	store.Apply(streamTestVessel(t, "V-01", 1))
	_, cursor := store.SnapshotCursor()
	_, sub, ok := store.Subscribe(cursor)
	if !ok {
		t.Fatal("subscribe was rejected")
	}
	sub.Close()
	if got := len(store.subscribers); got != 0 {
		t.Fatalf("subscriber was not released, %d remain", got)
	}
}

func TestReplaceInvalidatesCursorsAndSubscribers(t *testing.T) {
	store := NewSnapshotStore()
	store.Apply(streamTestVessel(t, "V-01", 1))
	_, cursor := store.SnapshotCursor()
	_, sub, ok := store.Subscribe(cursor)
	if !ok {
		t.Fatal("subscribe was rejected")
	}
	defer sub.Close()

	store.Replace([]model.Vessel{streamTestVessel(t, "V-02", 9)}, time.Now())
	select {
	case <-sub.Lagged():
	default:
		t.Fatal("replace did not push subscribers to resync")
	}
	if _, _, ok := store.Subscribe(cursor); ok {
		t.Fatal("pre-replace cursor survived a replace")
	}
}
