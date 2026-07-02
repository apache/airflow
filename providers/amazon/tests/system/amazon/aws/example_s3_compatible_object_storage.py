#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
Example Dag: input -> transform -> output on an S3-compatible object store via ``ObjectStoragePath``.

The Amazon provider talks to any S3-compatible object store, so ``ObjectStoragePath("s3://...")``
reaches the store through the amazon provider once the ``aws`` connection points at its S3
endpoint. Amazon S3 is the baseline; the same code works against other S3-compatible services
(for example Amazon S3, Backblaze B2, Cloudflare R2, and MinIO). See the recipe "Use an
S3-compatible object store for Airflow remote task logs" for connection and ``[logging]`` setup.

Set up an ``aws`` connection (default id ``aws_s3``) whose ``extra`` includes the S3
``endpoint_url`` and ``region_name``. The bucket name comes from ``S3_BUCKET_NAME``.

Requires the s3fs extra: ``pip install 'apache-airflow-providers-amazon[s3fs]'``.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.sdk import ObjectStoragePath, dag, task

DAG_ID = "example_s3_compatible_object_storage"

# Connection id and bucket are read from the environment so the example carries no secrets.
S3_CONN_ID = os.environ.get("S3_CONN_ID", "aws_s3")
S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME", "my-bucket")

# ObjectStoragePath resolves the connection lazily, so building it at module scope is safe.
base = ObjectStoragePath(f"s3://{S3_CONN_ID}@{S3_BUCKET_NAME}/airflow-demo")


@dag(
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "s3-compatible", "object-storage"],
)
def example_s3_compatible_object_storage():
    """Write input to the object store, transform it, and write the output back."""

    @task
    def input_to_store() -> str:
        """Write a raw input object to the store and return its path."""
        base.mkdir(exist_ok=True)
        src = base / "input.txt"
        src.write_text("s3\ncompatible\nobject\nstorage\n")
        return str(src)

    @task
    def transform(src_path: str) -> str:
        """Read the input from the store, uppercase it, and write the result back."""
        src = ObjectStoragePath(src_path)
        text = src.read_text()
        dst = base / "output.txt"
        dst.write_text(text.upper())
        return str(dst)

    @task
    def output_from_store(dst_path: str) -> None:
        """Read the transformed object back from the store to confirm the round-trip."""
        dst = ObjectStoragePath(dst_path)
        print(dst.read_text())

    output_from_store(transform(input_to_store()))


dag = example_s3_compatible_object_storage()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
