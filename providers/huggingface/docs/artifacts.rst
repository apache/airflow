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
    
Dataset artifact contract
=========================

Load and map tasks write a JSON manifest at the requested ``output_uri`` and
one Parquet object per split beside it (``<uri>.split-N.parquet``). The
returned reference contains the manifest URI, its SHA-256 fingerprint, source
revision, split names, row counts, and serialized Hugging Face ``Features``.
Downstream tasks should pass the complete reference, rather than only its URI,
to retain fingerprint validation and provenance.

The manifest and per-split SHA-256 checksums detect changed or incomplete
artifacts. The manifest preserves column names and order, records each
Dataset's split name, and retains DatasetDict split keys. The reader copies
each Parquet object to a local temporary file
while hashing it, then opens the verified copy with ``Dataset.from_parquet``.
This avoids reading a remote split twice and keeps the returned Dataset backed
by its local Arrow cache files, so mapping tasks can process it without first
holding the whole split in memory. The artifact writer enforces a hard feature
allowlist before writing: only ``Value`` and ``ClassLabel`` features,
recursively nested in ``Sequence``, list/tuple, and dictionary structures,
are supported. ``Image``, ``Audio``, ``Video``, ``Array2D`` and other array
features, and every other feature class are rejected with ``ValueError``.
Empty splits are restored from their declared feature schema, since some
Datasets versions cannot read zero-row Parquet files with ``from_parquet``.

The format is designed for ``file://``, ``s3://``, and ``gs://`` paths through
Airflow ``ObjectStoragePath``. Remote schemes need their matching Airflow
storage provider and filesystem dependency installed on every worker. Put the
matching Airflow connection ID in the URI's user-info section, such as
``s3://aws_default@my-bucket/path/data.json`` or
``gs://google_cloud_default@my-bucket/path/data.json``. A bare URI without a
connection ID uses the filesystem's default credential chain; configuring an
Airflow connection alone does not select it. Local files require a shared path
if tasks run on different workers. A failed write can leave an incomplete
manifest or orphaned split objects; rerunning the writer overwrites those
objects. Configure object-store lifecycle cleanup for abandoned temporary
objects. Alternatively, set an operator's ``storage_conn_id`` to select the
Airflow connection for its artifact paths. Use either ``storage_conn_id`` or a
connection ID in the URI's user-info section, but not both: precedence differs
between supported Airflow versions. Publication checks the manifest fingerprint
before upload.

The canonical Airflow asset URI for a dataset repository is
``hf://datasets/<repo_id>``. Hugging Face Hub supplies ``HfFileSystem`` for
``hf://`` file access; Airflow asset URI registration is a separate feature
and does not enable or configure that filesystem.
