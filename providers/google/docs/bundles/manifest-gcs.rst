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

Manifest GCS bundles
####################

The :class:`~airflow.providers.google.cloud.bundles.manifest_gcs.ManifestGCSDagBundle`
reads a mutable GCS source and publishes immutable, content-addressed snapshots.
Airflow parses and executes validated local cache copies of those snapshots. Task
retries, deferred tasks, callbacks, and reruns that retain their bundle version can
recover the same files even after the source changes or the local cache is removed.
This bundle requires Apache Airflow 3.5 or later.

Configure automatic publication
===============================

Install the Google provider on each Dag processor and worker. Configure the bundle
in ``[dag_processor] dag_bundle_config_list``:

.. code-block:: json

    [
      {
        "name": "gcs-production",
        "classpath": "airflow.providers.google.cloud.bundles.manifest_gcs.ManifestGCSDagBundle",
        "kwargs": {
          "bucket_name": "airflow-source",
          "prefix": "dags/",
          "gcp_conn_id": "google_cloud_default",
          "published_root": "gs://airflow-releases/production",
          "published_root_conn_id": "google_cloud_default",
          "refresh_interval": 30,
          "source_stability_seconds": 30,
          "deployment_marker_key": ".ready"
        }
      }
    ]

Both connections default to ``google_cloud_default``. Use
``published_root_conn_id`` to give the artifact store separate credentials from
the read-only source connection. A shared filesystem path is also supported for
``published_root``; provision that directory first and omit
``published_root_conn_id``. Every consumer must see the same published root.

Keep source and publication prefixes separate. A separate bucket is simplest;
disjoint prefixes in one bucket also work. Never include the release artifacts
in the mutable source.

For a GCS published root, bundle links in the UI point to the immutable snapshot
for the requested version. A filesystem published root has no default browsing
URL; configure ``view_url_template`` if your deployment provides one.

The bundle downloads the exact object generations observed in GCS. It excludes
``.git``, ``__pycache__``, ``.pyc`` files, and its own manifest. Downloads go into
a disposable mirror which Airflow never parses. Unchanged objects are reused;
unsafe or corrupt mirrors are rebuilt. Publication checks the source again before
replacing the release reference.

Source objects must be stored without ``Content-Encoding`` (or with ``identity``).
Encoded objects, including gzip-encoded Python files, are rejected before publication.
Upload their uncompressed contents without gzip upload flags. This does not restrict
ordinary binary files such as archives stored without a content-encoding header.

The source must remain unchanged for ``source_stability_seconds``, which defaults
to ``refresh_interval``. Processor replicas share the stability candidate in the
published root. Keep their clocks synchronized. Before the first release is ready,
the processor retries initialization; later failures keep the existing release.
Pinned bundle instances never publish or read the mutable source.

The default limits are 10,000 source files, 100 MiB per file, and 1 GiB total.
Configure ``max_file_count``, ``max_file_size_bytes``, and ``max_total_size_bytes``
to change them. Empty sources fail publication unless ``allow_empty_source`` is true.

Define a deployment boundary
============================

A quiet source is not necessarily a complete deployment. With
``deployment_marker_key=".ready"``, publish a deployment as follows:

1. Remove the existing marker before changing any source files.
2. Upload the new Dag files below the source prefix and delete obsolete files.
3. Write the marker last, using a new deployment identifier, after all changes succeed.

The marker is relative to the source prefix and is excluded from snapshots. The
bundle checks it before and after listing. While the marker is absent, the previous
release stays active; before the first release, initialization reports that the
bundle is not ready. Source changes under an already published marker are also
rejected.

Serialize deployments to this source and remove the marker at the start of every
deployment, including retries. Retaining an old marker is unsafe: if Airflow misses
a completed deployment between refreshes, that marker could authorize files from a
later, incomplete deployment. A marker is a completion signal, not an inventory of
the files it authorizes. All publishers of one bundle must use the same source,
marker key, and publication mode.

Explicit publication
====================

Set ``auto_publish`` to false when a deployment pipeline controls publication.
Load the configured bundle and call
:func:`~airflow.providers.google.cloud.bundles.manifest_gcs.publish_manifest_gcs_dag_bundle`:

.. code-block:: python

    from airflow.dag_processing.bundles.manager import DagBundlesManager
    from airflow.providers.google.cloud.bundles.manifest_gcs import publish_manifest_gcs_dag_bundle

    bundle = DagBundlesManager().get_bundle("gcs-production")
    result = publish_manifest_gcs_dag_bundle(bundle=bundle)
    print(result.version)

For subsequent releases, pass ``expected_current_version`` with the version your
deployment intends to replace. A stale publisher then fails instead of replacing
a newer release. Automatic bundles and pinned instances reject explicit publication.
The publisher does not access the metadata database.

Storage, permissions, and retention
==================================

A GCS published root contains immutable snapshots under
``versions/<bundle>/sha256-<hash>/``, a release reference at
``refs/<bundle>/latest.json``, and shared candidate state at
``_state/<bundle>/auto-publish.json``. Snapshot manifests are committed last;
references and candidate state use generation-match conditional writes. Concurrent
publishers preserve the first committed snapshot manifest. When possible, GCS
copies source objects on the server, pinned to their observed generations; failed
copies fall back to uploading the verified local mirror.

Source readers need ``storage.buckets.get``, ``storage.objects.list``, and
``storage.objects.get``. Publishers additionally need ``storage.objects.get``,
``storage.objects.create``, and ``storage.objects.delete`` on the releases prefix;
GCS requires delete permission to replace mutable documents. Pinned workers and
explicit-mode processors need only ``storage.objects.get`` on the published root.
Include any permissions required by customer-managed encryption keys.

Airflow cleans its disposable local cache, but never deletes published snapshots.
Keep them for as long as a Dag run can request its recorded version. Back up the
published root and configure lifecycle rules accordingly; the mutable source
cannot reconstruct a historical release after it changes.
