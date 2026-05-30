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

import textwrap
from pathlib import Path

import check_runtime_ti_protocol_sync as mod
from check_runtime_ti_protocol_sync import (
    check_sync,
)


def _write(tmp_path: Path, filename: str, impl_body: str, proto_body: str) -> tuple[Path, Path]:
    impl = tmp_path / filename
    impl.write_text(f"class RuntimeTaskInstance:\n{textwrap.indent(textwrap.dedent(impl_body), '    ')}\n")
    proto = tmp_path / "types.py"
    proto.write_text(
        f"class RuntimeTaskInstanceProtocol:\n{textwrap.indent(textwrap.dedent(proto_body), '    ')}\n"
    )
    return impl, proto


class TestAllowList:
    def test_suppresses_detection(self, tmp_path):
        """Members in IMPLEMENTATION_ONLY must not trigger a failure even when absent from Protocol."""
        impl, proto = _write(tmp_path, "task_runner.py", "bundle_instance: object\n", "pass\n")
        original = mod.IMPLEMENTATION_ONLY
        mod.IMPLEMENTATION_ONLY = {"bundle_instance"}
        try:
            assert check_sync(impl, proto) is True
        finally:
            mod.IMPLEMENTATION_ONLY = original


class TestDriftDetection:
    def test_new_field_in_impl(self, tmp_path):
        impl, proto = _write(tmp_path, "task_runner.py", "is_mapped: bool | None = None\n", "pass\n")
        assert check_sync(impl, proto) is False

    def test_new_method_in_impl(self, tmp_path):
        impl, proto = _write(
            tmp_path,
            "task_runner.py",
            "def render_templates(self, context, jinja_env): ...\n",
            "pass\n",
        )
        assert check_sync(impl, proto) is False

    def test_new_property_in_impl(self, tmp_path):
        impl, proto = _write(
            tmp_path,
            "task_runner.py",
            "@property\ndef log_url(self) -> str: ...\n",
            "pass\n",
        )
        assert check_sync(impl, proto) is False

    def test_mismatch_in_signature_is_detected(self, tmp_path):
        impl, proto = _write(
            tmp_path,
            "task_runner.py",
            "def get_first_reschedule_date(self, context): ...\n",
            "def get_first_reschedule_date(self, first_try_number): ...\n",
        )
        assert check_sync(impl, proto) is False
