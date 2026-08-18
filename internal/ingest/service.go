// Package ingest accepts call-completion webhooks and processes them.
package ingest

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/store"
)

// recordingWork stands in for downloading and transcoding a recording.
const recordingWork = 50 * time.Millisecond

// Service ingests webhook deliveries.
type Service struct {
	store *store.Store
	cache *stats.Cache
	rdb   *redis.Client
	log   *slog.Logger

	// wg tracks background recording jobs so they can finish before
	// the process exits during graceful shutdown.
	wg sync.WaitGroup
}

// New builds a Service.
func New(
	s *store.Store,
	c *stats.Cache,
	rdb *redis.Client,
	log *slog.Logger,
) *Service {
	return &Service{
		store: s,
		cache: c,
		rdb:   rdb,
		log:   log,
	}
}

// Stats returns the cached totals for an account.
func (s *Service) Stats(accountID string) stats.AccountStats {
	return s.cache.Get(accountID)
}

// Ingest stores a delivery and kicks off processing.
// Processing runs asynchronously so the provider gets a fast acknowledgement.
func (s *Service) Ingest(ctx context.Context, evt Event) error {
	payload, err := json.Marshal(evt)
	if err != nil {
		return err
	}

	rec := store.Event{
		EventID:      evt.EventID,
		CallID:       evt.CallID,
		AccountID:    evt.AccountID,
		Status:       evt.Status,
		DurationSec:  evt.DurationSec,
		RecordingURL: evt.RecordingURL,
		OccurredAt:   evt.OccurredAt,
		Payload:      payload,
	}

	// IngestEvent performs the duplicate check and all database writes
	// atomically inside PostgreSQL.
	//
	// This prevents concurrent duplicate deliveries from being processed
	// more than once and prevents partial writes if one operation fails.
	inserted, err := s.store.IngestEvent(ctx, rec)
	if err != nil {
		return err
	}

	// The event already exists. Treat the redelivery as successfully handled.
	if !inserted {
		s.log.Info(
			"duplicate delivery ignored",
			"event_id", evt.EventID,
		)
		return nil
	}

	// Keep the in-memory cache in sync only after the durable database
	// transaction has successfully committed.
	s.cache.Record(rec.AccountID, rec.DurationSec)

	// Recordings are slow to fetch, so that work does not block the
	// provider's webhook acknowledgement.
	if rec.RecordingURL != "" {
		s.wg.Add(1)

		go func() {
			defer s.wg.Done()

			if err := s.processRecording(rec); err != nil {
				s.log.Error(
					"recording processing failed",
					"event_id", rec.EventID,
					"call_id", rec.CallID,
					"err", err,
				)
			}
		}()
	}

	return nil
}

// processRecording downloads and transcodes the call recording,
// then marks the call as processed.
func (s *Service) processRecording(rec store.Event) error {
	time.Sleep(recordingWork)

	// Do not use the HTTP request context here. The request may already
	// have completed or been cancelled by the time this background work
	// finishes.
	return s.store.MarkRecordingProcessed(
		context.Background(),
		rec.CallID,
	)
}

// Shutdown waits for all currently running background recording jobs
// to finish before the service exits.
func (s *Service) Shutdown(ctx context.Context) error {
	done := make(chan struct{})

	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
