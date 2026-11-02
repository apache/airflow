Operators
=========

The initial public operator API is ``LoadDatasetOperator``,
``MapDatasetOperator``, ``StreamDatasetOperator``, and
``PushToHubOperator``. The operators exchange the manifest and split Parquet
objects described in :doc:`artifacts`.

LoadDatasetOperator
-------------------

``LoadDatasetOperator`` accepts ``path``, ``output_uri``, optional ``name``
(dataset configuration), ``split``, ``revision``, ``cache_dir``,
``download_mode``, ``load_kwargs``, ``storage_conn_id``, and ``durable``
(default ``True``). ``storage_conn_id`` selects the Airflow object-storage
connection for ``output_uri``; if omitted, a connection ID in the URI's
user-info section or the backend's default credentials are used. It
persists the result and returns the
artifact URI plus source ID, resolved revision, configuration, split, row
counts, and feature descriptions. It resolves a Hub source to a commit before
loading; pin a commit SHA in ``revision`` for reproducible inputs.

With ``durable=True``, the operator stores the resolved source revision in
Airflow's task state store so retries use the same Hub commit. Set it to
``False`` to resolve the revision again on each attempt without task-state
reuse.

The default ``reuse_dataset_if_exists`` mode reuses Datasets' valid cache.
``force_redownload`` forces a new download. ``cache_dir`` isolates cache data;
it is separate from the output artifact destination. Keeping a stable cache
directory allows retries and cleared task instances to reuse downloaded data.

MapDatasetOperator
------------------

Required parameters are ``input_uri``, ``output_uri``, and ``transform``.
``transform`` must be a Python callable available when the DAG is parsed and
when the task worker executes. The operator does not template or import the
callable from a string. ``transform_kwargs`` are passed as Datasets ``fn_kwargs``;
``map_kwargs`` are passed to ``Dataset.map`` or ``DatasetDict.map``. The output
preserves compatible provenance and records a new artifact fingerprint.
``storage_conn_id`` selects the connection used for both input and output
artifact paths.

StreamDatasetOperator
---------------------

Hugging Face Datasets ``streaming=True`` returns an ``IterableDataset`` (or an
iterable of splits) that reads examples incrementally. It avoids materializing
the full source dataset and is intended for sequential iteration; it does not
provide the usual in-memory ``Dataset`` behavior such as random access.

``StreamDatasetOperator`` loads with
``streaming=True``, applies the optional ``transform`` with
``transform_kwargs``, then applies the optional ``predicate`` using Datasets'
lazy iterable operations. It passes the resulting ``IterableDataset`` or
``IterableDatasetDict`` directly to ``push_to_hub`` in the same task. Datasets
progressively writes Parquet shards to the Hub, so rows are not accumulated in
a Python list, stored in XCom, or written to a local artifact first.

The operator accepts ``path``, ``repo_id``, optional ``name``, ``split``,
``revision``, ``config``, ``private``, ``commit_message``,
``huggingface_conn_id``, ``push_kwargs``, and ``durable`` (default ``True``).
With durability enabled, it pins the resolved Hub source revision and reuses a
completed push from task state or the matching final Hub commit on retry. Set
``durable=False`` to disable those reuse checks. It returns the Hub repository,
commit SHA and URL, source revision, configuration, split, and
``streaming=True``. After a successful push it emits the matching
``hf://datasets/<repo_id>`` asset event. A run marker in the commit message
identifies the source and target for this DAG run. A retry searches the target
branch or pull request refs and accepts only a final upload commit; intermediate
parts of a multi-commit upload do not count as completed publication.

Execution proceeds in this order:

#. **Pin the source.** For a Hub dataset, the operator resolves the requested
   revision to a commit SHA. With ``durable=True``, it stores that SHA in the
   task state store under a key for the source path, requested revision,
   configuration, and split. A retry reuses the SHA, so a moving branch such as
   ``main`` does not silently change between attempts. Non-Hub paths are passed
   to Datasets without Hub revision lookup.
#. **Check for a completed publication.** The operator builds a marker from
   the task and run IDs, source and pinned revision, configuration and split,
   destination repository, target config/revision, and pull-request setting.
   With durability enabled, it first checks task state, then searches the target
   Hub refs for a completed commit carrying that marker. A match skips loading
   the source and pushing again; the prior result and asset event are reused.
#. **Create a lazy stream if recovery found no commit.** The operator calls
   ``load_dataset(..., streaming=True)`` and passes the optional configuration,
   split, and pinned revision. Hugging Face Datasets reads rows incrementally.
   The optional ``transform`` is applied with ``IterableDataset.map`` and
   ``transform_kwargs``; the optional ``predicate`` is then applied with
   ``IterableDataset.filter``. Neither operation materializes the entire
   dataset first.
#. **Publish directly to the Hub.** The transformed iterable is passed to
   ``push_to_hub`` in the same task. ``config``, ``private``, and ``push_kwargs``
   control the destination. The commit message includes the recovery marker.
   Datasets uploads Parquet shards progressively; the operator does not collect
   the rows into a list or write an intermediate artifact.
#. **Recover and report the commit.** If the push returns a commit, its SHA and
   URL are returned. If it returns no commit object and durability is enabled,
   the operator searches the Hub again in case the upload succeeded before the
   worker stopped. It accepts only a final upload part. If no completed commit
   can be found, the task raises ``RuntimeError``. The result contains commit
   metadata and source provenance, not dataset rows; that result is saved to
   task state when available and copied to the outlet event's extra fields.

Unlike the local Parquet artifact path, this operator delegates feature
serialization to Datasets' Hub upload implementation and does not use the
provider's ``_validate_feature`` whitelist. It does not create a local manifest
reference for downstream ``MapDatasetOperator`` or ``PushToHubOperator`` tasks;
downstream tasks should load the published Hub repository. Passing
``streaming=True`` through ``LoadDatasetOperator.load_kwargs`` is not a
substitute: that operator writes a durable artifact and its writer accepts
``Dataset`` and ``DatasetDict``, not ``IterableDataset``.

For example:

.. code-block:: python

   from airflow.providers.huggingface.operators.datasets import StreamDatasetOperator

   publish_stream = StreamDatasetOperator(
       task_id="publish_stream",
       path="cornell-movie-review-data/rotten_tomatoes",
       split="train",
       repo_id="my-org/rotten-tomatoes-clean",
       transform=normalize_review,
       predicate=keep_nonempty_review,
   )

PushToHubOperator
-----------------

Required parameters are ``input_uri`` (the full artifact reference, including
fingerprint) and ``repo_id``. ``config``, ``private``, ``commit_message``,
``huggingface_conn_id``, ``storage_conn_id``, and ``push_kwargs`` configure
publication. ``storage_conn_id`` selects the connection used to read the
artifact. The
operator accepts ``durable`` (default ``True``) in addition to its publication
options. It verifies the manifest fingerprint and per-split checksums before
uploading. With durability enabled it checks Airflow task state and the target
Hub revision for a completed publication before loading Parquet data; a retry
can therefore return the prior result even if the artifact is no longer on the
worker. Set ``durable=False`` to always load and push the artifact without
state or Hub-marker reuse. It returns ``repository_id``, the Hub commit identifier as
``commit_sha``, the commit URL as ``commit_url``, the artifact fingerprint,
and source provenance. Both live ``CommitInfo`` and recovered
``GitCommitInfo`` records are supported. Hub exceptions propagate without
wrapping.

The commit message includes an artifact fingerprint and target marker. The
marker distinguishes repository configuration, revision, and pull request
target, so publishing the same artifact to another target is a separate
publication. Airflow 3.3 and later task state store support caches the result
when that facility is available. Otherwise a retry searches the intended
branch or pull request refs and only reuses the final part of a multi-commit
upload. This lets it recover a completed push if the worker exited before
saving task state without treating an intermediate part as complete. The task
emits an asset event with the same commit SHA and URL only after successful
publication. Publication uses Datasets ``push_to_hub``, which rewrites the
README's YAML card metadata, including ``dataset_info`` and ``configs``, in the
push commit.

Example
-------

.. code-block:: python

   from airflow.providers.huggingface.operators.datasets import (
       LoadDatasetOperator, MapDatasetOperator, PushToHubOperator,
   )
   from my_project.transforms import normalize

   load = LoadDatasetOperator(
       task_id="load", path="nyu-mll/glue", name="mrpc", split="train",
       revision="<commit-sha>", output_uri="s3://airflow/data/{{ dag_id }}/{{ run_id }}/mrpc.json",
       storage_conn_id="aws_default",
   )
   transform = MapDatasetOperator(
       task_id="normalize", input_uri=load.output,
       output_uri="s3://airflow/data/{{ dag_id }}/{{ run_id }}/mrpc-normalized.json", transform=normalize,
       storage_conn_id="aws_default",
   )
   publish = PushToHubOperator(
       task_id="publish", input_uri=transform.output,
       repo_id="my-org/mrpc-normalized",
       storage_conn_id="aws_default",
   )
   load >> transform >> publish
