 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
    
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
connection for ``output_uri``. If omitted, a connection ID in the URI's
user-info section or the backend's default credentials are used. Set only one
of ``storage_conn_id`` and a URI user-info connection ID, because precedence
differs between supported Airflow versions. It
persists the result and returns the
artifact URI plus source ID, resolved revision, configuration, split, row
counts, and feature descriptions. It resolves a Hub source to a commit before
loading; pin a commit SHA in ``revision`` for reproducible inputs. Unnamespaced
IDs such as ``imdb`` are treated as Hub repositories unless they name an
existing local path or a packaged Datasets builder such as ``csv`` or
``parquet``.

With ``durable=True``, the operator stores the resolved source revision in
Airflow's task state store so retries use the same Hub commit. Set it to
``False`` to resolve the revision again on each attempt without task-state
reuse. The task state store is available in Airflow 3.3 and later; on earlier
versions, ``durable=True`` logs a warning and cannot pin the revision across
retries.

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
artifact paths. Use it or connection IDs in the artifact URIs' user-info
sections, but not both, because precedence differs between supported Airflow
versions.

The system example demonstrates the durable artifact flow and its task handoff:

.. exampleinclude:: /../../huggingface/tests/system/huggingface/example_read_only.py
    :language: python
    :start-after: [START huggingface_load_and_map_example]
    :end-before: [END huggingface_load_and_map_example]

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
With durability enabled on Airflow 3.3 and later, it pins the resolved Hub
source revision and reuses a completed push from task state. It searches Hub
commit markers only after task state records that a push may have started.
Markers include the Dag, task, run, map index, source, and transform identity.
Set ``durable=False`` to disable those reuse checks. It returns the Hub repository,
commit SHA and URL, source revision, configuration, split, and
``streaming=True``. After a successful push it emits the matching
``hf://datasets/<repo_id>`` asset event. A run marker in the commit message
identifies the source and target for this DAG run. A retry searches the target
branch or pull request refs only when task state says a prior attempt may have
started a push. Recovery accepts the latest commit on the target ref only when
it carries this marker and has no multipart suffix. Intermediate upload parts
and older historical commits do not count as completed publication.
Mapping is supported when ``repo_id`` is fixed in ``partial()``; the mapped
Dag then declares the same repository-level asset as an ordinary task. A
``repo_id`` supplied through ``expand()`` is not supported because the asset
must be known when the Dag is serialized. Pull request publications and pushes
to revisions other than ``main`` do not emit this repo-level asset event,
because it would schedule consumers against ``main`` rather than the published
revision.

Execution proceeds in this order:

#. **Pin the source.** For a Hub dataset, the operator resolves the requested
   revision to a commit SHA. With ``durable=True``, it stores that SHA in the
   task state store under a key for the source path, requested revision,
   configuration, and split. A retry reuses the SHA, so a moving branch such as
   ``main`` does not silently change between attempts. Non-Hub paths are passed
   to Datasets without Hub revision lookup.
#. **Check for a completed publication.** The operator builds a marker from the
   Dag, task, run, map index, source and pinned revision, configuration and split,
   transform and predicate identities, destination repository, target
   config/revision, and pull-request setting. On Airflow 3.3 and later, it checks
   task state first. It searches Hub refs only when task state records that a
   prior attempt may have started the push. A matching final commit recovers an
   interrupted publication. Clearing the task removes that marker state, so a
   cleared task starts a new publication.
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
   URL are returned. If it returns no commit object and task state is available,
   the operator searches the Hub in case the upload succeeded before the
   worker stopped. It accepts only a final upload part. If no completed commit
   can be found, the task raises ``RuntimeError``. The result contains commit
   metadata and source provenance, not dataset rows; that result is saved to
   task state when available and copied to the outlet event's extra fields.

Unlike the local Parquet artifact path, this operator delegates feature
serialization to Datasets' Hub upload implementation and does not use the
provider's ``_validate_feature`` allowlist. It does not create a local manifest
reference for downstream ``MapDatasetOperator`` or ``PushToHubOperator`` tasks;
downstream tasks should load the published Hub repository. Passing
``streaming=True`` through ``LoadDatasetOperator.load_kwargs`` is not a
substitute: that operator writes a durable artifact and its writer accepts
``Dataset`` and ``DatasetDict``, not ``IterableDataset``.


PushToHubOperator
-----------------

Required parameters are ``input_uri`` (the full artifact reference, including
fingerprint) and ``repo_id``. ``config``, ``private``, ``commit_message``,
``huggingface_conn_id``, ``storage_conn_id``, and ``push_kwargs`` configure
publication. ``storage_conn_id`` selects the connection used to read the
artifact. The
operator accepts ``durable`` (default ``True``) in addition to its publication
options. Task state store support for result caching and retry recovery requires
Airflow 3.3 or later; older versions log a warning. It verifies the manifest
fingerprint and per-split checksums before uploading. With durability enabled,
it checks task state and searches the latest target-ref commit only when task
state records that an earlier attempt may have started the push. A retry can
therefore return the prior result even if the artifact is no longer on the
worker, provided the completed commit is still latest on the target ref. Set
``durable=False`` to always load and push the artifact without
state or Hub-marker reuse. It returns ``repository_id``, the Hub commit identifier as
``commit_sha``, the commit URL as ``commit_url``, the artifact fingerprint,
and source provenance. Both live ``CommitInfo`` and recovered
``GitCommitInfo`` records are supported. Hub exceptions propagate without
wrapping.

The commit message includes an artifact fingerprint and target marker. The
marker distinguishes repository configuration, revision, pull request target,
and all ``push_kwargs`` (including ``split`` and ``data_dir``), so publishing
the same artifact to another target is a separate publication. Airflow 3.3 and
later task state store support caches the result
and records that a push may have started. A retry searches the intended branch
or pull request ref only after that record exists, and only reuses the latest
unsuffixed commit carrying the marker. This avoids reusing an older publication
after the branch has advanced. The task
emits an asset event with the same commit SHA and URL only after successful
publication. Publication uses Datasets ``push_to_hub``, which rewrites the
README's YAML card metadata, including ``dataset_info`` and ``configs``, in the
push commit. Only a publication to ``main`` without ``create_pr`` emits the
repo-level asset event; pull request and non-main revision pushes do not change
the revision referenced by ``hf://datasets/<repo_id>``.

Example
-------

The system example publishes a mapped artifact to a temporary private Hub
repository and checks the resulting commit and asset event:

.. exampleinclude:: /../../huggingface/tests/system/huggingface/example_write.py
    :language: python
    :start-after: [START huggingface_map_and_publish_example]
    :end-before: [END huggingface_map_and_publish_example]
