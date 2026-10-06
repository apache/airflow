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
from __future__ import annotations

import hashlib

import pytest
from task_sdk.coordinators.executable._bundle_test_utils import ENTRYPOINT_PATH, write_bundle

from airflow.sdk.coordinators.executable._bundle_reader import (
    read_bundle_entrypoint_source,
    read_bundle_language,
    read_bundle_source,
)
from airflow.sdk.coordinators.executable.coordinator import _digest_cache

from tests_common.test_utils.version_compat import AIRFLOW_V_3_3_PLUS

if not AIRFLOW_V_3_3_PLUS:
    pytest.skip("Coordinator is only compatible with Airflow >= 3.3.0", allow_module_level=True)

ORDERS = b'package main\n\nfunc orders() { dag.New("orders") }\n'
REPORTS = b'package dags\n\nfunc reports() { dag.New("reports") }\n'
MAIN = b"package main\n\nfunc main() {}\n"
REPORTS_PATH = "example/bundle/dags/reports.go"
TWO_FILES = {ENTRYPOINT_PATH: ORDERS, REPORTS_PATH: REPORTS}


@pytest.fixture(autouse=True)
def _clear_digest_cache():
    _digest_cache.clear()


def _entry(source_path: str, content: bytes, offset: int = 0, **overrides) -> dict:
    return {
        "path": source_path,
        "offset": offset,
        "length": len(content),
        "sha256": hashlib.sha256(content).hexdigest(),
        **overrides,
    }


class TestReadBundleSource:
    def test_returns_the_file_of_a_mapped_dag(self, tmp_path):
        bundle = write_bundle(
            tmp_path / "b",
            "orders",
            "reports",
            sources=TWO_FILES,
            dag_source_paths={"orders": ENTRYPOINT_PATH, "reports": REPORTS_PATH},
        )

        assert read_bundle_source(bundle, "orders") == ORDERS.decode()
        assert read_bundle_source(bundle, "reports") == REPORTS.decode()

    def test_falls_back_to_the_entrypoint_for_an_unmapped_dag(self, tmp_path):
        bundle = write_bundle(tmp_path / "b", "orders", sources=TWO_FILES, dag_source_paths={})

        assert read_bundle_source(bundle, "dynamic") == ORDERS.decode()

    def test_returns_none_without_a_dag_id(self, tmp_path):
        bundle = write_bundle(tmp_path / "b", "orders")

        assert read_bundle_source(bundle) is None

    def test_returns_none_without_sources(self, tmp_path):
        bundle = write_bundle(tmp_path / "b", "orders", omit_sources=True)

        assert read_bundle_source(bundle, "orders") is None
        assert read_bundle_entrypoint_source(bundle) is None

    def test_returns_none_without_an_entrypoint(self, tmp_path):
        bundle = write_bundle(tmp_path / "b", "orders", sources={}, entrypoint_path=None, dag_source_paths={})

        assert read_bundle_source(bundle, "orders") is None

    def test_decodes_utf8(self, tmp_path):
        bundle = write_bundle(tmp_path / "b", "orders", sources={ENTRYPOINT_PATH: "// café\n".encode()})

        assert read_bundle_entrypoint_source(bundle) == "// café\n"


class TestReadBundleEntrypointSource:
    def test_returns_the_entrypoint_not_a_dag_file(self, tmp_path):
        bundle = write_bundle(
            tmp_path / "b",
            "reports",
            sources={ENTRYPOINT_PATH: MAIN, REPORTS_PATH: REPORTS},
            dag_source_paths={"reports": REPORTS_PATH},
        )

        assert read_bundle_entrypoint_source(bundle) == MAIN.decode()


class TestReadBundleLanguage:
    def test_returns_the_sdk_language(self, tmp_path):
        assert read_bundle_language(write_bundle(tmp_path / "b", "orders", language="rust")) == "rust"

    def test_returns_none_without_one(self, tmp_path):
        assert read_bundle_language(write_bundle(tmp_path / "b", "orders", language=None)) is None


class TestInvalidSources:
    def test_rejects_a_file_that_is_not_a_bundle(self, tmp_path):
        path = tmp_path / "plain"
        path.write_bytes(b"not a bundle")

        with pytest.raises(ValueError, match="is not a valid executable bundle"):
            read_bundle_entrypoint_source(path)

    def test_rejects_duplicate_paths(self, tmp_path):
        index = [_entry(ENTRYPOINT_PATH, MAIN), _entry(ENTRYPOINT_PATH, MAIN)]
        bundle = write_bundle(tmp_path / "b", "orders", index=index)

        with pytest.raises(ValueError, match="duplicate source path"):
            read_bundle_entrypoint_source(bundle)

    @pytest.mark.parametrize(
        ("overrides", "message"),
        [
            ({"offset": 1}, "extends past the source region"),
            ({"length": len(MAIN) + 1}, "extends past the source region"),
            ({"offset": -1}, "offset must be a non-negative integer"),
            ({"length": -1}, "length must be a non-negative integer"),
            ({"offset": "0"}, "offset must be a non-negative integer"),
            ({"length": True}, "length must be a non-negative integer"),
            ({"sha256": hashlib.sha256(MAIN).hexdigest().upper()}, "64 lowercase hexadecimal digits"),
            ({"path": ""}, "path must be a non-empty string"),
        ],
    )
    def test_rejects_a_malformed_region(self, tmp_path, overrides, message):
        bundle = write_bundle(tmp_path / "b", "orders", index=[_entry(ENTRYPOINT_PATH, MAIN, **overrides)])

        with pytest.raises(ValueError, match=message):
            read_bundle_entrypoint_source(bundle)

    def test_rejects_a_digest_mismatch(self, tmp_path):
        bundle = write_bundle(
            tmp_path / "b", "orders", index=[_entry(ENTRYPOINT_PATH, MAIN, sha256="0" * 64)]
        )

        with pytest.raises(ValueError, match="SHA-256 mismatch"):
            read_bundle_entrypoint_source(bundle)

    def test_rejects_a_digest_mismatch_in_a_file_that_is_not_read(self, tmp_path):
        bundle = write_bundle(
            tmp_path / "b",
            "orders",
            sources=TWO_FILES,
            index=[
                _entry(ENTRYPOINT_PATH, ORDERS),
                _entry(REPORTS_PATH, REPORTS, len(ORDERS), sha256="0" * 64),
            ],
        )

        with pytest.raises(ValueError, match="SHA-256 mismatch"):
            read_bundle_entrypoint_source(bundle)

    def test_rejects_an_entrypoint_missing_from_sources(self, tmp_path):
        bundle = write_bundle(tmp_path / "b", "orders", entrypoint_path="other/main.go")

        with pytest.raises(ValueError, match="entrypoint_path 'other/main.go' is not one of its sources"):
            read_bundle_entrypoint_source(bundle)

    def test_rejects_a_dag_mapped_to_a_file_missing_from_sources(self, tmp_path):
        bundle = write_bundle(tmp_path / "b", "orders", dag_source_paths={"orders": "missing.go"})

        with pytest.raises(ValueError, match="maps 'orders' to 'missing.go', not a source"):
            read_bundle_source(bundle, "orders")

    def test_rejects_invalid_utf8(self, tmp_path):
        bundle = write_bundle(tmp_path / "b", "orders", sources={ENTRYPOINT_PATH: b"\xff\xfe"})

        with pytest.raises(ValueError, match="not valid UTF-8"):
            read_bundle_entrypoint_source(bundle)

    def test_rejects_a_non_list_sources(self, tmp_path):
        bundle = write_bundle(tmp_path / "b", "orders", index={"a": 1})  # type: ignore[arg-type]

        with pytest.raises(ValueError, match="sources must be a list"):
            read_bundle_entrypoint_source(bundle)
