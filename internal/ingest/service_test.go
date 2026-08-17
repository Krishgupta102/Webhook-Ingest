package ingest_test

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/convin/webhook-ingest/internal/testutil"
)

// eventJSON builds a well-formed call-completion payload.
func eventJSON(eventID, callID, accountID string) string {
	return fmt.Sprintf(`{
	  "event_id":      %q,
	  "call_id":       %q,
	  "account_id":    %q,
	  "status":        "completed",
	  "duration_sec":  143,
	  "recording_url": "https://recordings.example.com/%s.wav",
	  "occurred_at":   "2026-08-13T09:12:00Z"
	}`, eventID, callID, accountID, callID)
}

func post(t *testing.T, url, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func TestWebhookStoresEventAndCall(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	exists, err := st.EventExists(ctx, eventID)
	if err != nil {
		t.Fatalf("EventExists: %v", err)
	}
	if !exists {
		t.Fatal("expected the event to be stored")
	}

	var gotAccount string
	row := st.Pool().QueryRow(ctx, `SELECT account_id FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&gotAccount); err != nil {
		t.Fatalf("expected a call record for %s: %v", callID, err)
	}
	if gotAccount != accountID {
		t.Fatalf("call belongs to %q, want %q", gotAccount, accountID)
	}
}

func TestConcurrentDuplicateDeliveryIsIgnored(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	body := eventJSON(eventID, callID, accountID)

	const deliveries = 20

	errCh := make(chan error, deliveries)

	for i := 0; i < deliveries; i++ {
		go func() {
			resp, err := http.Post(
				srv.URL+"/webhooks/calls",
				"application/json",
				strings.NewReader(body),
			)
			if err != nil {
				errCh <- err
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				errCh <- fmt.Errorf("got status %d, want 200", resp.StatusCode)
				return
			}

			errCh <- nil
		}()
	}

	for i := 0; i < deliveries; i++ {
		if err := <-errCh; err != nil {
			t.Fatal(err)
		}
	}

	ctx := context.Background()

	var eventCount int
	err := st.Pool().QueryRow(
		ctx,
		`SELECT count(*) FROM events WHERE event_id = $1`,
		eventID,
	).Scan(&eventCount)
	if err != nil {
		t.Fatalf("count events: %v", err)
	}

	if eventCount != 1 {
		t.Fatalf("stored %d copies of %s, want 1", eventCount, eventID)
	}

	var statsCount int
	err = st.Pool().QueryRow(
		ctx,
		`SELECT call_count FROM account_stats WHERE account_id = $1`,
		accountID,
	).Scan(&statsCount)
	if err != nil {
		t.Fatalf("get account stats: %v", err)
	}

	if statsCount != 1 {
		t.Fatalf("account stats count = %d, want 1", statsCount)
	}

	var callCount int
	err = st.Pool().QueryRow(
		ctx,
		`SELECT count(*) FROM calls WHERE call_id = $1`,
		callID,
	).Scan(&callCount)
	if err != nil {
		t.Fatalf("count calls: %v", err)
	}

	if callCount != 1 {
		t.Fatalf("stored %d calls, want 1", callCount)
	}
}
