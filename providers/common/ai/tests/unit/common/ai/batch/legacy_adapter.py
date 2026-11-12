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
"""An out-of-tree adapter written against the original ``BatchAdapter`` surface, for the compatibility tests."""

from __future__ import annotations

from airflow.providers.common.ai.batch.base import BatchAdapter, BatchState, ExtractedOutput, SubmitResult


class LegacyAdapter(BatchAdapter):
    """Implements only what existed before pydantic-ai handles: no handle, no async members, no ``from_connection``."""

    name = "acme"
    max_requests = 10
    allows_per_request_model = False
    max_payload_bytes = 1_000

    def __init__(self, *, api_key=None, base_url=None, client=None):
        super().__init__(api_key=api_key, base_url=base_url, client=client)
        self.api_key = api_key
        self.base_url = base_url
        self.polled: list[str] = []
        self.cancelled: list[str] = []

    def validate_requests(self, requests, *, model, output_spec, idempotency_key, **kwargs):
        pass

    def submit(self, requests, *, model, idempotency_key, input_fingerprint, output_spec, **kwargs):
        return SubmitResult(batch_id="old-batch", provider_input_ref=None)

    def get_batch(self, batch_id):
        self.polled.append(batch_id)
        return BatchState(status="completed", counts=None, error_message=None)

    def cancel_batch(self, batch_id):
        self.cancelled.append(batch_id)

    def iter_results(self, batch_id):
        return iter(())

    def extract_output(self, raw, spec):
        return ExtractedOutput(kind="text", text="")

    def find_orphaned_batch(self, idempotency_key, input_fingerprint, not_before):
        return None
