// Package ingest accepts call-completion webhooks and processes them.
package ingest

import (
	"context"
	"encoding/json"
	"log/slog"
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
//
// The event, call record, and account statistics are persisted
// atomically by the store. Duplicate deliveries are ignored.
//
// Recording processing runs asynchronously so the provider gets
// a fast acknowledgement.
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

	// Store the event, call record, and account statistics
	// in one database transaction.
	inserted, err := s.store.IngestEvent(ctx, rec)
	if err != nil {
		return err
	}

	// PostgreSQL's unique constraint guarantees that only one
	// concurrent delivery can insert a given event_id.
	if !inserted {
		s.log.Info(
			"duplicate delivery ignored",
			"event_id", evt.EventID,
		)
		return nil
	}

	// Keep the in-memory cache synchronized with the durable
	// database aggregate.
	s.cache.Record(
		rec.AccountID,
		rec.DurationSec,
	)

	// Recordings are slow to fetch, so that part does not block
	// the provider from receiving the webhook acknowledgement.
	if rec.RecordingURL != "" {
		go func() {
			if err := s.processRecording(ctx, rec); err != nil {
				s.log.Error(
					"recording processing failed",
					"call_id", rec.CallID,
					"error", err,
				)
			}
		}()
	}

	return nil
}

// processRecording downloads and transcodes the call recording,
// then marks the call as done.
func (s *Service) processRecording(
	ctx context.Context,
	rec store.Event,
) error {
	time.Sleep(recordingWork)

	return s.store.MarkRecordingProcessed(
		ctx,
		rec.CallID,
	)
}
