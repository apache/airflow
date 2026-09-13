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

Bundles
#######

Dag bundles allow Airflow to load Dags from external sources. For a general overview see
:doc:`apache-airflow:administration-and-deployment/dag-bundles`.

S3DagBundle
===========

Use the :class:`~airflow.providers.amazon.aws.bundles.s3.S3DagBundle` to configure an S3 bundle in your Airflow's
``[dag_processor] dag_bundle_config_list``.

Example of using the S3DagBundle:

**JSON format example**:

.. code-block:: bash

    export AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST='[
      {
        "name": "my-s3-dags",
        "classpath": "airflow.providers.amazon.aws.bundles.s3.S3DagBundle",
        "kwargs": {
          "aws_conn_id": "aws_default",
          "bucket_name": "my-airflow-bucket",
          "prefix": "dags/",
          "refresh_interval": 60
        }
      }
    ]'

Without ``manifest_key``, the bundle keeps its original mutable behavior and synchronizes the latest objects under
``prefix``. Airflow does not pin Dag runs in this mode.

Versioned, atomic deployments
-----------------------------

Airflow 3.4 and later can pin an S3 Dag bundle to an immutable deployment. Enable this mode by configuring a
publisher-managed current pointer key:

.. code-block:: json

    {
      "name": "my-versioned-s3-dags",
      "classpath": "airflow.providers.amazon.aws.bundles.s3.S3DagBundle",
      "kwargs": {
        "aws_conn_id": "aws_default",
        "bucket_name": "my-airflow-bucket",
        "prefix": "dags/",
        "manifest_key": "airflow-bundles/current.json",
        "refresh_interval": 60
      }
    }

The bucket must have S3 Versioning enabled. A release manifest names every bundle object by its exact S3
``VersionId`` and records its size and SHA-256 digest:

.. code-block:: json

    {
      "schema_version": 1,
      "bucket_name": "my-airflow-bucket",
      "prefix": "dags",
      "objects": [
        {
          "key": "dags/example.py",
          "version_id": "3Lg...",
          "size": 418,
          "sha256": "d7a8fbb307d7809469ca9abcb0082e4f8d5651e46d3cdb762d02d0bf37c9e592"
        }
      ]
    }

Every ``version_id`` must be a nonempty, non-``null`` S3 version. The current pointer and the entire
``<manifest_key>.releases/`` metadata namespace are reserved and must not appear in ``objects``.
Schema version 1 is strict: a release has exactly the four fields shown above, each object has exactly its four
shown fields, and a pointer has exactly ``schema_version`` and ``bundle_version``. Additional fields require a new
schema version and are rejected by this reader.

The bundle version is the lowercase SHA-256 digest of the canonical release manifest. To produce the canonical
bytes, normalize ``prefix`` by removing its optional trailing slash, sort ``objects`` by ``key``, then encode the
whole release object as UTF-8 JSON with keys sorted, no insignificant whitespace, and non-ASCII characters left
unescaped. For example:

.. code-block:: python

    import hashlib
    import json

    release["prefix"] = release["prefix"].rstrip("/")
    release["objects"] = sorted(release["objects"], key=lambda item: item["key"])
    canonical = json.dumps(
        release,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    bundle_version = hashlib.sha256(canonical).hexdigest()

Upload deployment artifacts in this order:

1. Upload every Dag and support file, recording the returned S3 ``VersionId``, byte size, and SHA-256 digest.
2. Upload the release manifest to
   ``<manifest_key>.releases/<bundle_version>.json``. Create this content-addressed key only if absent (for
   example with ``If-None-Match: *``), or verify that an existing object has identical canonical content.
3. Last, atomically replace ``manifest_key`` with the current pointer:

   .. code-block:: json

       {"schema_version":1,"bundle_version":"<64-character lowercase SHA-256>"}

Publishing the pointer last is the deployment transaction boundary. Uploading objects or a release manifest alone
does not make them visible to Airflow. A rollback only requires publishing a pointer to an earlier retained release.
Airflow reads the pointer once per refresh. When the trusted local generation is absent, it validates the
content-addressed release, downloads each exact object version into a staging directory, verifies size and SHA-256,
and publishes the complete local generation with one rename. Cached pinned runs skip both pointer and release reads.
A missing, malformed, or incomplete release leaves the previous generation active.

Airflow verifies source integrity when it first publishes a generation. Later task startups use the atomic
completion marker in Airflow's private ``dag_bundle_storage_path`` instead of rereading the entire bundle. Protect
that local directory as trusted Airflow state.

Pinned Dag runs resolve their release directly from the recorded bundle version and do not depend on mutable
``version_data`` in the metadata database. The configured ``bucket_name``, ``prefix``, and ``manifest_key`` must
therefore remain stable for a bundle name until every run, retry, and callback that could need an old release is
past the retention horizon. Relocate only after that horizon, then use a new bundle name. Keeping old and new
bundle configurations active at the same time is only safe when they cannot expose duplicate Dag IDs.

Retention and permissions
-------------------------

Retain every release manifest and every referenced noncurrent object version for at least the maximum Dag run,
retry, clearing, and backfill horizon. An S3 lifecycle policy that deletes either artifact prevents recovering the
corresponding historical generation on a cache miss or a new worker. Protect the ``.releases/`` namespace from
overwrite where possible; Airflow still recomputes its content hash and rejects mismatches.

The AWS connection needs permission to check the bucket and read the current pointer, release manifests, and exact
object versions. This normally includes ``s3:ListBucket``, ``s3:GetObject``, and ``s3:GetObjectVersion`` on the
configured bucket and keys. Set ``requester_pays`` to ``true`` when the bucket uses Requester Pays.
