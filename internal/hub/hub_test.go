package hub

import (
	"errors"
	"testing"
	"time"
)

const testTimeout = time.Second

// recvEvent reads one event off a subscription, failing the test instead of
// hanging forever if the publisher side has a bug.
func recvEvent(t *testing.T, sub *Subscription) (Event, bool) {
	t.Helper()
	select {
	case ev, open := <-sub.Events():
		return ev, open
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for event")
		return Event{}, false
	}
}

func TestPublishAssignsSequentialIDsAndDelivers(t *testing.T) {
	s := newStream("s", 0)
	sub, gap := s.Subscribe(0, 0)
	defer sub.Close()
	if gap {
		t.Fatal("unexpected gap on a fresh stream")
	}

	for i, data := range []string{"one", "two", "three"} {
		ev, err := s.Publish(data)
		if err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
		if ev.ID != uint64(i+1) {
			t.Fatalf("publish %d: got id %d, want %d", i, ev.ID, i+1)
		}

		got, open := recvEvent(t, sub)
		if !open {
			t.Fatalf("publish %d: subscription closed early", i)
		}
		if got.ID != ev.ID || got.Data != data {
			t.Fatalf("publish %d: got %+v, want id=%d data=%q", i, got, ev.ID, data)
		}
	}
}

func TestSubscribeReplaysBufferedEvents(t *testing.T) {
	s := newStream("s", 0)
	for _, data := range []string{"a", "b", "c"} {
		if _, err := s.Publish(data); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}

	sub, gap := s.Subscribe(0, 0)
	defer sub.Close()
	if gap {
		t.Fatal("unexpected gap: nothing was ever evicted")
	}

	for i, want := range []string{"a", "b", "c"} {
		got, open := recvEvent(t, sub)
		if !open {
			t.Fatalf("replay %d: subscription closed early", i)
		}
		if got.Data != want {
			t.Fatalf("replay %d: got %q, want %q", i, got.Data, want)
		}
	}
}

func TestSubscribeReplaysOnlyEventsAfterLastID(t *testing.T) {
	s := newStream("s", 0)
	for _, data := range []string{"a", "b", "c"} {
		if _, err := s.Publish(data); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}

	sub, gap := s.Subscribe(1, 0)
	defer sub.Close()
	if gap {
		t.Fatal("unexpected gap: lastID is still within the buffer")
	}

	for i, want := range []string{"b", "c"} {
		got, open := recvEvent(t, sub)
		if !open {
			t.Fatalf("replay %d: subscription closed early", i)
		}
		if got.Data != want {
			t.Fatalf("replay %d: got %q, want %q", i, got.Data, want)
		}
	}
}

func TestSubscribeReportsGapWhenHistoryWasEvicted(t *testing.T) {
	s := newStream("s", 2)
	for _, data := range []string{"a", "b", "c", "d"} {
		if _, err := s.Publish(data); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	// Capacity 2 means only ids 3 and 4 ("c", "d") remain buffered.

	sub, gap := s.Subscribe(1, 0)
	defer sub.Close()
	if !gap {
		t.Fatal("expected a gap: events 2 and 3 were evicted before id 1's successor")
	}

	got, open := recvEvent(t, sub)
	if !open {
		t.Fatal("subscription closed early")
	}
	if got.Data != "c" {
		t.Fatalf("first replayed event = %q, want %q", got.Data, "c")
	}
}

func TestPublishDropsSlowSubscriberAsLagged(t *testing.T) {
	s := newStream("s", 0)
	sub, _ := s.Subscribe(0, 1)
	defer sub.Close()

	// The subscriber's channel holds 1 event; the second publish finds it
	// still full and must drop the subscriber rather than block.
	if _, err := s.Publish("one"); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if _, err := s.Publish("two"); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Drain the one event that made it through before the drop.
	if _, open := recvEvent(t, sub); !open {
		t.Fatal("expected the buffered event before the lag drop")
	}

	if _, open := recvEvent(t, sub); open {
		t.Fatal("expected the channel to close once the subscriber lagged")
	}
	if !errors.Is(sub.Err(), ErrLagged) {
		t.Fatalf("Err() = %v, want ErrLagged", sub.Err())
	}
}

func TestPublishAfterFinishReturnsErrStreamDone(t *testing.T) {
	s := newStream("s", 0)
	s.Finish()

	if _, err := s.Publish("late"); !errors.Is(err, ErrStreamDone) {
		t.Fatalf("Publish after Finish: got %v, want ErrStreamDone", err)
	}
}

func TestFinishClosesLiveSubscribersCleanly(t *testing.T) {
	s := newStream("s", 0)
	sub, _ := s.Subscribe(0, 0)
	defer sub.Close()

	if _, err := s.Publish("last"); err != nil {
		t.Fatalf("publish: %v", err)
	}
	s.Finish()

	got, open := recvEvent(t, sub)
	if !open || got.Data != "last" {
		t.Fatalf("expected the published event before close, got %+v open=%v", got, open)
	}
	if _, open := recvEvent(t, sub); open {
		t.Fatal("expected the channel to close after Finish")
	}
	if err := sub.Err(); err != nil {
		t.Fatalf("Err() after a normal Finish = %v, want nil", err)
	}
	if !s.Done() {
		t.Fatal("Done() should be true after Finish")
	}
}

func TestSubscribeAfterFinishDeliversHistoryThenCloses(t *testing.T) {
	s := newStream("s", 0)
	if _, err := s.Publish("only"); err != nil {
		t.Fatalf("publish: %v", err)
	}
	s.Finish()

	sub, gap := s.Subscribe(0, 0)
	defer sub.Close()
	if gap {
		t.Fatal("unexpected gap")
	}

	got, open := recvEvent(t, sub)
	if !open || got.Data != "only" {
		t.Fatalf("expected buffered history, got %+v open=%v", got, open)
	}
	if _, open := recvEvent(t, sub); open {
		t.Fatal("expected the channel to close immediately after the history")
	}
}

func TestHubGetOrCreateReturnsSameStream(t *testing.T) {
	h := New(0)
	a := h.GetOrCreate("x")
	b := h.GetOrCreate("x")
	if a != b {
		t.Fatal("GetOrCreate returned different streams for the same id")
	}
	if _, ok := h.Stream("missing"); ok {
		t.Fatal("Stream found an id that was never created")
	}
}

func TestHubRemoveFinishesAndForgetsStream(t *testing.T) {
	h := New(0)
	s := h.GetOrCreate("x")
	sub, _ := s.Subscribe(0, 0)
	defer sub.Close()

	if !h.Remove("x") {
		t.Fatal("Remove reported no such stream")
	}
	if h.Remove("x") {
		t.Fatal("second Remove should report the stream is already gone")
	}
	if _, ok := h.Stream("x"); ok {
		t.Fatal("removed stream is still reachable")
	}
	if _, open := recvEvent(t, sub); open {
		t.Fatal("Remove should finish the stream and close its subscribers")
	}
}

func TestHubIDsSorted(t *testing.T) {
	h := New(0)
	h.GetOrCreate("charlie")
	h.GetOrCreate("alpha")
	h.GetOrCreate("bravo")

	got := h.IDs()
	want := []string{"alpha", "bravo", "charlie"}
	if len(got) != len(want) {
		t.Fatalf("IDs() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("IDs() = %v, want %v", got, want)
		}
	}
}
