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
"""Fault-injection tests for triggerer callback bundle registration: initialize() raises."""

from __future__ import annotations

import sys
from unittest import mock

from airflow.utils.file import get_unique_dag_module_name


class TestBundleInitializeRaises:
    def test_initialize_raises_does_not_crash_or_pollute_syspath(self, tmp_path):
        """
        A corrupt bundle whose initialize() raises must be skipped gracefully:
        no exception bubbles, and because the sys.path.append is reached only AFTER
        a file match (which is gated behind initialize()), nothing is added.
        """
        from airflow.triggers.callback import _ensure_bundle_module_registered

        mod_name = "unusual_prefix_" + "b" * 40 + "_corrupt_dag"

        bad_bundle = mock.Mock()
        bad_bundle.name = "corrupt-bundle"
        bad_bundle.path = "/should/never/be/added"
        bad_bundle.initialize.side_effect = RuntimeError("corrupt bundle, cannot init")

        before = list(sys.path)
        with mock.patch("airflow.triggers.callback.DagBundlesManager") as mgr:
            mgr.return_value.get_all_dag_bundles.return_value = [bad_bundle]
            _ensure_bundle_module_registered(f"{mod_name}.fn")  # must not raise

        assert mod_name not in sys.modules
        assert sys.path == before, "failed bundle init must not leave a path on sys.path"

    def test_one_bad_bundle_does_not_block_good_bundle(self, tmp_path):
        """initialize() raising on bundle A must not prevent registration from bundle B."""
        from airflow.triggers.callback import _ensure_bundle_module_registered

        stem = "good_dag"
        good_dir = tmp_path / "good"
        good_dir.mkdir()
        (good_dir / f"{stem}.py").write_text("OK = True\n")
        mod_name = get_unique_dag_module_name(str(good_dir / f"{stem}.py"))

        bad_bundle = mock.Mock()
        bad_bundle.name = "bad"
        bad_bundle.path = "/nope"
        bad_bundle.initialize.side_effect = RuntimeError("boom")

        good_bundle = mock.Mock()
        good_bundle.name = "good"
        good_bundle.path = good_dir

        before = list(sys.path)
        try:
            with mock.patch("airflow.triggers.callback.DagBundlesManager") as mgr:
                mgr.return_value.get_all_dag_bundles.return_value = [bad_bundle, good_bundle]
                _ensure_bundle_module_registered(f"{mod_name}.fn")
            assert mod_name in sys.modules
            assert str(good_dir) in sys.path
            assert "/nope" not in sys.path
        finally:
            sys.modules.pop(mod_name, None)
            sys.path[:] = before

    def test_load_mangled_raises_after_path_append_is_swallowed(self, tmp_path):
        """
        If load_mangled_dag_module raises AFTER the sys.path.append (the fix #1 site),
        the outer except must swallow it. The appended path is the REAL matched bundle
        dir (not a bogus path), so leaving it is acceptable; key invariant: no crash.
        """
        from airflow.triggers.callback import _ensure_bundle_module_registered

        stem = "raising_dag"
        (tmp_path / f"{stem}.py").write_text("X = 1\n")
        mod_name = get_unique_dag_module_name(str(tmp_path / f"{stem}.py"))

        bundle = mock.Mock()
        bundle.name = "b"
        bundle.path = tmp_path

        before = list(sys.path)
        try:
            with (
                mock.patch("airflow.triggers.callback.DagBundlesManager") as mgr,
                mock.patch(
                    "airflow.triggers.callback.load_mangled_dag_module",
                    side_effect=ImportError("cannot exec module"),
                ),
            ):
                mgr.return_value.get_all_dag_bundles.return_value = [bundle]
                _ensure_bundle_module_registered(f"{mod_name}.fn")  # must not raise
            # The matched bundle path may remain on sys.path (it is a real dir), but the
            # module must NOT be registered since loading failed.
            assert mod_name not in sys.modules
        finally:
            sys.modules.pop(mod_name, None)
            sys.path[:] = before
