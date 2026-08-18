# Solution

## What was broken

The service had three main reliability problems.

### 1. Duplicate webhook deliveries were not handled atomically

The original ingestion flow checked whether an event existed and then inserted it as two separate operations:

1. Check `EventExists`
2. Insert the event
3. Update the call
4. Increment account statistics

Under concurrent delivery, two requests could both observe that the event did not exist before either inserted it. Both requests could then continue processing the same event, resulting in duplicate event rows and incorrect account statistics.

The fix uses PostgreSQL as the source of truth for idempotency. `events.event_id` now has a unique constraint/index, and the event insertion uses:

`INSERT ... ON CONFLICT (event_id) DO NOTHING`

The service only performs the call/stat updates when the event was actually inserted.

### 2. Database writes could be partially applied

The event, call record, and account statistics were originally written as separate database operations. If a later operation failed after the event had already been inserted, the database could be left in a partially updated state.

The ingestion path was changed so the event insertion, call upsert, and account-stat update are performed within one PostgreSQL transaction.

This means the database changes either all commit or none of them do.

### 3. Recording processing could disappear after the request ended

Recording processing was started in a goroutine using the HTTP request context.
Once the HTTP request completed or its context was cancelled, the background
operation could be cancelled before marking the recording as processed.

The background processing was changed to use its own context instead of the
request context, and recording workers are tracked so graceful shutdown waits
for in-flight recording work to finish. Processing failures are logged instead
of being silently discarded.

For a larger production system, recording work should eventually be moved to
a durable queue so that work survives process crashes and deployments rather
than relying only on graceful shutdown.

## Deduplication strategy

I chose PostgreSQL rather than Redis or an in-memory lock because PostgreSQL is already the durable source of truth for webhook events.

The `event_id` supplied by the provider is stable across redeliveries, so it is a natural idempotency key.

A unique database constraint guarantees correctness even when multiple webhook requests arrive concurrently. The database, rather than application-level locking, decides which delivery wins.

Redis could be used for a fast deduplication layer, but it would add another failure mode and would still require a durable database constraint to guarantee correctness.

An in-memory map or mutex would not work reliably across multiple service instances or after a restart.

## Scaling to 10,000 webhooks/sec

At 10,000 webhooks/sec, I would move toward a queue-based architecture.

The HTTP service would validate the webhook and enqueue it quickly, returning an acknowledgement after durable acceptance. Worker processes would consume events from a durable message broker and perform database processing asynchronously.

I would also:

- Batch database writes where possible.
- Partition or shard high-volume event data.
- Use connection-pool limits appropriate for the database.
- Keep PostgreSQL's unique constraint as the final idempotency guarantee.
- Use Redis for caching rather than correctness-critical deduplication.
- Add metrics for ingestion latency, queue depth, processing failures, duplicate rate, and database latency.
- Add retry handling and a dead-letter queue for failed processing.
- Separate recording processing from the webhook ingestion path so slow recordings cannot affect webhook throughput.
- Run multiple stateless webhook instances behind a load balancer.