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

"""Fake google-cloud-storage client for GCS artifact-store tests.

Implements just enough of the client/bucket/blob surface for
``GCSArtifactStore``: generation-based conditional uploads, downloads that
report the served generation, existence checks, and generation-pinned rewrites.
Objects are keyed by (bucket, name) so rewrite sources can live in a different
bucket than the store's.
"""

from __future__ import annotations

import io
from types import SimpleNamespace


class FakeGCSStoreError(Exception):
    def __init__(self, code: int, message: str = "") -> None:
        super().__init__(message or str(code))
        self.code = code


class FakeStoreGCSBlob:
    def __init__(
        self,
        client: FakeStoreGCSClient,
        bucket_name: str,
        name: str,
        generation: int | None = None,
    ) -> None:
        self._client = client
        self._bucket_name = bucket_name
        self.name = name
        self.generation = generation

    def _record(self) -> tuple[bytes, int] | None:
        return self._client.records.get((self._bucket_name, self.name))

    def download_as_bytes(self) -> bytes:
        if self._client.missing_buckets and self._bucket_name in self._client.missing_buckets:
            raise FakeGCSStoreError(404, "The specified bucket does not exist")
        record = self._record()
        if record is None:
            raise FakeGCSStoreError(404, f"No such object: {self._bucket_name}/{self.name}")
        body, generation = record
        if self.generation is not None and self.generation != generation:
            raise FakeGCSStoreError(404, f"No such generation: {self.generation}")
        self.generation = generation
        return body

    def reload(self) -> None:
        if self._client.missing_buckets and self._bucket_name in self._client.missing_buckets:
            raise FakeGCSStoreError(404, "The specified bucket does not exist")
        record = self._record()
        if record is None:
            raise FakeGCSStoreError(404, f"No such object: {self._bucket_name}/{self.name}")
        self.generation = record[1]

    def open(self, mode: str = "rb", *, raw_download: bool = False):
        assert mode == "rb"
        return io.BytesIO(self.download_as_bytes())

    def upload_from_string(
        self,
        body,
        content_type: str | None = None,
        if_generation_match: int | None = None,
        retry=None,
    ) -> None:
        client = self._client
        if self._client.missing_buckets and self._bucket_name in self._client.missing_buckets:
            raise FakeGCSStoreError(404, "The specified bucket does not exist")
        if if_generation_match is not None and client.fail_conditional_writes_with:
            raise FakeGCSStoreError(client.fail_conditional_writes_with, "injected failure")
        key = (self._bucket_name, self.name)
        record = client.records.get(key)
        if if_generation_match is not None:
            current_generation = record[1] if record is not None else 0
            if if_generation_match != current_generation:
                raise FakeGCSStoreError(412, "conditionNotMet")
        data = body.encode("utf-8") if isinstance(body, str) else bytes(body)
        generation = client._next_generation()
        client.records[key] = (data, generation)
        self.generation = generation
        client.put_sequence.append(self.name)
        client.put_conditions.append((self.name, if_generation_match))

    def rewrite(
        self,
        source: FakeStoreGCSBlob,
        token: str | None = None,
        if_source_generation_match: int | None = None,
    ):
        client = self._client
        client.rewrite_attempts += 1
        if client.fail_rewrite_with is not None:
            raise FakeGCSStoreError(client.fail_rewrite_with, "rewrite failed")
        record = client.records.get((source._bucket_name, source.name))
        if record is None:
            raise FakeGCSStoreError(404, f"No such object: {source._bucket_name}/{source.name}")
        body, generation = record
        if source.generation is not None and source.generation != generation:
            raise FakeGCSStoreError(404, f"No such generation: {source.generation}")
        if if_source_generation_match is not None and if_source_generation_match != generation:
            raise FakeGCSStoreError(412, "conditionNotMet")
        new_generation = client._next_generation()
        client.records[(self._bucket_name, self.name)] = (body, new_generation)
        self.generation = new_generation
        client.rewrites.append((source._bucket_name, source.name, self.name))
        return None, len(body), len(body)


class FakeStoreGCSBucket:
    def __init__(self, client: FakeStoreGCSClient, name: str) -> None:
        self._client = client
        self.name = name

    def blob(self, name: str, generation: int | None = None) -> FakeStoreGCSBlob:
        return FakeStoreGCSBlob(self._client, self.name, name, generation=generation)


class FakeStoreGCSClient:
    def __init__(self, *, endpoint: str = "https://storage.googleapis.com") -> None:
        self._connection = SimpleNamespace(API_BASE_URL=endpoint)
        #: (bucket, name) -> (body, generation)
        self.records: dict[tuple[str, str], tuple[bytes, int]] = {}
        self.put_sequence: list[str] = []
        self.put_conditions: list[tuple[str, int | None]] = []
        self.rewrites: list[tuple[str, str, str]] = []
        self.rewrite_attempts = 0
        self.fail_rewrite_with: int | None = None
        self.fail_conditional_writes_with: int | None = None
        self.missing_buckets: set[str] = set()
        self._generation_counter = 0

    def _next_generation(self) -> int:
        self._generation_counter += 1
        return self._generation_counter

    def bucket(self, name: str) -> FakeStoreGCSBucket:
        return FakeStoreGCSBucket(self, name)

    # -- test helpers (external writers that bypass this client's bookkeeping) --

    def put(self, key: str, body: bytes, *, bucket: str) -> int:
        generation = self._next_generation()
        self.records[(bucket, key)] = (body, generation)
        return generation

    def pop(self, key: str, *, bucket: str) -> None:
        del self.records[(bucket, key)]

    def get(self, key: str, *, bucket: str) -> bytes:
        return self.records[(bucket, key)][0]

    def generation_of(self, key: str, *, bucket: str) -> int:
        return self.records[(bucket, key)][1]
