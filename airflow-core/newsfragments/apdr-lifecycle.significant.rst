Partitioned asset scheduling holds pending rows until event links commit

Producers reusing a pending partitioned asset run now hold a row lock until commit.
The lock is shared when the incoming event carries no partition date and exclusive
when it carries a date. Dated producers using the same consumer key therefore
serialize, which can increase contention for those workloads.

The scheduler's claim of these pending rows takes its row lock even when
``[scheduler] use_row_level_locking=False``. This coordinates producers with the
scheduler as well as coordinating multiple schedulers.

Public ``POST /assets/events`` listeners now run after the event commits. A listener
failure no longer rolls back that event. Task-completion and public asset-event
transactions retry database deadlocks up to three attempts; exhausted retries
propagate the original deadlock. Other database errors are not replayed.

Replay is in-process and does not provide crash-safe callback delivery. Metrics
and tracing may be emitted by more than one attempt. The lifecycle guarantee
requires participating producers and scheduler claims; direct deletes outside
that protocol and mixed-version operation are not covered. SQLite does not
provide the concurrent-writer guarantee.

This follow-up adds no migration and does not repair historical pending duplicates.
