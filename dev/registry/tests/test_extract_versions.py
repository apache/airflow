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
"""Unit tests for dev/registry/extract_versions.py."""

from __future__ import annotations

from extract_versions import (
    AIRFLOW_ROOT,
    PROVIDERS_JSON_CANDIDATES,
    SCRIPT_DIR,
)


class TestProvidersJsonCandidates:
    """Lock down the lookup order for providers.json.

    Regression test for the workflow path mismatch where the
    `Download existing providers.json` step writes to dev/registry/ but
    the script previously only checked the eleventy data dir.
    """

    def test_dev_registry_path_first(self):
        # The workflow's S3 download writes here; checked first so CI runs
        # work without having to do a full local extract pass first.
        assert PROVIDERS_JSON_CANDIDATES[0] == SCRIPT_DIR / "providers.json"

    def test_eleventy_data_dir_fallback(self):
        # Local dev convenience: after a full extract pass, the data dir
        # has providers.json and we don't need to also keep a copy in
        # dev/registry/.
        assert PROVIDERS_JSON_CANDIDATES[1] == AIRFLOW_ROOT / "registry" / "src" / "_data" / "providers.json"

    def test_no_other_candidates(self):
        # Adding silently to the list could mask path mismatches that
        # should be caught and fixed at the source. Match siblings (extract_
        # parameters.py, extract_metadata.py) which use exactly these two.
        assert len(PROVIDERS_JSON_CANDIDATES) == 2
