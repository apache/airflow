AIP-76: Asset Partitioning

Airflow 3.2 introduces asset partitioning (time-based), allowing Dags to produce and consume
partitioned asset events identified by a ``partition_key`` (e.g. ``2026-03-10T09`` for
an hourly partition).

**New timetables**:

- ``CronPartitionTimetable`` — schedules a producer Dag on a cron expression and emits a
  partitioned asset event on each run.
- ``PartitionedAssetTimetable`` — schedules a consumer Dag when the expected partitioned
  asset events have arrived.

**Partition mappers** transform upstream partition keys before passing them to downstream Dags:

- ``IdentityMapper`` — passes the key through unchanged (default).
- ``StartOfHourMapper``, ``StartOfDayMapper``, ``StartOfWeekMapper``, ``StartOfMonthMapper``,
  ``StartOfQuarterMapper``, ``StartOfYearMapper`` — normalize a datetime partition key to a
  coarser time granularity (e.g. ``StartOfDayMapper`` maps ``2026-03-10T09:30:00`` → ``2026-03-10``).
- ``ProductMapper`` — applies a separate mapper to each ``|``-delimited segment of a
  composite key (e.g. ``ProductMapper(IdentityMapper(), StartOfDayMapper())`` maps
  ``"us|2026-03-10T09:30:00"`` → ``"us|2026-03-10"``).
- ``ChainMapper`` — applies mappers sequentially, passing each step's output to the next (e.g.,
  ``ChainMapper(StartOfHourMapper(), StartOfDayMapper(input_format="%Y-%m-%dT%H"))`` maps
  ``"2026-03-10T09:30:00"`` → ``"2026-03-10"``).
- ``AllowedKeyMapper`` — validates the key is in a fixed allowlist and passes it through
  unchanged, raising ``ValueError`` otherwise (e.g.
  ``AllowedKeyMapper(["us", "eu", "ap"])`` accepts ``"us"`` but rejects ``"cn"``).

Mappers can be set globally on a ``PartitionedAssetTimetable`` or overridden per upstream asset via ``partition_mapper_config``.

Within the task context, the ``partition_key`` is available as ``dag_run.partition_key``. It can also be provided when manually triggering a Dag run via the REST API (``POST /dags/{dag_id}/dagRuns``).

* Migration rules needed

  * None — asset partitioning is a new feature. Existing Dags and assets are unaffected
