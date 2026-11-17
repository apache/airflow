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
from typing import Any
from unittest import mock

import pytest
import structlog.testing

pytest.importorskip("airflow.dag_processing.bundles", reason="Requires Airflow 3+")

from airflow.dag_processing.bundles.base import BaseDagBundle as CoreBaseDagBundle
from airflow.providers.common.compat import bundles as compat_bundles
from airflow.providers.common.compat.bundles import BaseDagBundle

from tests_common.test_utils.config import conf_vars


def _make_bundle_class(base, log_context: dict[str, Any] | None = None):
    """Return a concrete bundle subclass with the given base, adding log_context to its logger if given."""

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

    if log_context is None:
        return _DummyBundle

    class _DummyBundleWithContext(_DummyBundle):
        def _get_log_context(self) -> dict[str, Any]:
            return log_context

    return _DummyBundleWithContext


@pytest.fixture(autouse=True)
def bundle_storage_path(tmp_path):
    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path)}):
        yield tmp_path


@pytest.fixture
def legacy_compat_base():
    """Yield the compat BaseDagBundle as it would appear on an older Airflow without _log."""
    with mock.patch.object(CoreBaseDagBundle, "_log", None, create=True):
        importlib.reload(compat_bundles)
        yield compat_bundles.BaseDagBundle
    # Reload again outside the patch so the real module is back in sys.modules.
    importlib.reload(compat_bundles)


class TestDagBundleCompat:
    def test_base_dag_bundle_is_subclass_of_core(self):
        assert issubclass(BaseDagBundle, CoreBaseDagBundle)

    def test_log_property_on_subclass_instance(self):
        """_log returns a bound structlog logger on concrete bundle subclasses."""
        bundle = _make_bundle_class(BaseDagBundle)(name="test-bundle")

        first = bundle._log
        assert first is not None
        assert bundle._log is first  # cached

    def test_log_is_bound_with_context(self):
        """_log includes bundle_name and version in its bound variables."""
        bundle = _make_bundle_class(BaseDagBundle)(name="my-bundle", version="abc123")

        with structlog.testing.capture_logs() as cap:
            bundle._log.info("test event")
        assert cap[0]["bundle_name"] == "my-bundle"
        assert cap[0]["version"] == "abc123"


class TestDagBundleCompatWithoutCoreLog:
    """The compat class on an Airflow whose BaseDagBundle has no _log."""

    def test_log_is_bound_with_bundle_name_and_version(self, legacy_compat_base):
        bundle = _make_bundle_class(legacy_compat_base)(name="legacy-bundle", version="abc123")

        with structlog.testing.capture_logs() as cap:
            bundle._log.info("test event")
        assert cap[0]["bundle_name"] == "legacy-bundle"
        assert cap[0]["version"] == "abc123"

    def test_log_includes_subclass_context(self, legacy_compat_base):
        bundle_class = _make_bundle_class(legacy_compat_base, log_context={"custom_key": "custom_value"})
        bundle = bundle_class(name="legacy-bundle")

        with structlog.testing.capture_logs() as cap:
            bundle._log.info("test event")
        assert cap[0]["custom_key"] == "custom_value"

    def test_log_is_cached(self, legacy_compat_base):
        bundle = _make_bundle_class(legacy_compat_base)(name="legacy-bundle")

        assert bundle._log is bundle._log

    def test_log_setter_allows_direct_assignment(self, legacy_compat_base):
        bundle = _make_bundle_class(legacy_compat_base)(name="legacy-bundle")
        custom_logger = structlog.get_logger("custom").bind(bundle_name="legacy-bundle")

        bundle._log = custom_logger

        assert bundle._log is custom_logger
