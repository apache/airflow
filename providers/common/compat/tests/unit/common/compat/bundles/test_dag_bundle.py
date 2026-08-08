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

import importlib
from pathlib import Path
from unittest import mock

import pytest
import structlog.testing

pytest.importorskip("airflow.dag_processing.bundles", reason="Requires Airflow 3+")

from airflow.dag_processing.bundles.base import BaseDagBundle as CoreBaseDagBundle
from airflow.providers.common.compat.bundles import BaseDagBundle

from tests_common.test_utils.config import conf_vars


def _make_bundle_class(base):
    """Return a concrete bundle subclass with the given base."""

    class _DummyBundle(base):
        @property
        def path(self) -> Path:
            return Path("/tmp")

        def initialize(self) -> None:
            pass

        def get_current_version(self):
            return None

        def refresh(self) -> None:
            pass

    return _DummyBundle


@pytest.fixture
def legacy_compat_base():
    """Yield the compat BaseDagBundle as it would appear on an older Airflow without _log."""
    import airflow.providers.common.compat.bundles as mod

    with mock.patch.object(CoreBaseDagBundle, "_log", None, create=True):
        importlib.reload(mod)
        yield mod.BaseDagBundle
    # Reload again outside the patch so the real module is back in sys.modules.
    importlib.reload(mod)


class TestDagBundleCompat:
    def test_base_dag_bundle_is_importable(self):
        assert BaseDagBundle is not None

    def test_base_dag_bundle_is_subclass_of_core(self):
        assert issubclass(BaseDagBundle, CoreBaseDagBundle)

    def test_has_log_property(self):
        assert hasattr(BaseDagBundle, "_log")

    def test_log_property_on_subclass_instance(self, tmp_path):
        """_log returns a bound structlog logger on concrete bundle subclasses."""
        DummyBundle = _make_bundle_class(BaseDagBundle)
        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path)}):
            bundle = DummyBundle(name="test-bundle")

        first = bundle._log
        assert first is not None
        assert bundle._log is first  # cached

    def test_log_is_bound_with_context(self, tmp_path):
        """_log includes bundle_name and version in its bound variables."""
        DummyBundle = _make_bundle_class(BaseDagBundle)
        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path)}):
            bundle = DummyBundle(name="my-bundle", version="abc123")

        with structlog.testing.capture_logs() as cap:
            bundle._log.info("test event")
        assert cap[0]["bundle_name"] == "my-bundle"
        assert cap[0]["version"] == "abc123"

    def test_compat_subclass_provides_log_when_missing(self, tmp_path, legacy_compat_base):
        """Even when the installed BaseDagBundle has no _log, the compat class fills it in."""
        DummyBundle = _make_bundle_class(legacy_compat_base)
        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path)}):
            bundle = DummyBundle(name="legacy-bundle")

        assert bundle._log is not None
