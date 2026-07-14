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

from unittest import mock

from airflow.dag_processing.bundles.base import BaseDagBundle as CoreBaseDagBundle
from airflow.providers.common.compat.bundles import BaseDagBundle


class TestDagBundleCompat:
    def test_base_dag_bundle_is_importable(self):
        assert BaseDagBundle is not None

    def test_base_dag_bundle_is_subclass_of_core(self):
        assert issubclass(BaseDagBundle, CoreBaseDagBundle)

    def test_has_log_property(self):
        assert hasattr(BaseDagBundle, "_log")

    def test_log_property_on_subclass_instance(self, tmp_path):
        """_log returns a bound structlog logger on concrete bundle subclasses."""
        from pathlib import Path

        from tests_common.test_utils.config import conf_vars

        class _DummyBundle(BaseDagBundle):
            @property
            def path(self) -> Path:
                return tmp_path

            def initialize(self) -> None:
                pass

            def get_current_version(self):
                return None

            def refresh(self) -> None:
                pass

        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path)}):
            bundle = _DummyBundle(name="test-bundle")

        log = bundle._log
        assert log is not None
        # Calling _log twice returns the same (cached) logger.
        assert bundle._log is log

    def test_log_is_bound_with_context(self, tmp_path):
        """_log includes bundle_name and version in its bound variables."""
        from pathlib import Path

        from tests_common.test_utils.config import conf_vars

        class _DummyBundle(BaseDagBundle):
            @property
            def path(self) -> Path:
                return tmp_path

            def initialize(self) -> None:
                pass

            def get_current_version(self):
                return None

            def refresh(self) -> None:
                pass

        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path)}):
            bundle = _DummyBundle(name="my-bundle", version="abc123")

        log = bundle._log
        # structlog bound loggers expose their bindings via _context
        ctx = log._context if hasattr(log, "_context") else {}
        assert ctx.get("bundle_name") == "my-bundle"
        assert ctx.get("version") == "abc123"

    def test_compat_subclass_provides_log_when_missing(self, tmp_path):
        """Even when the installed BaseDagBundle has no _log, the compat class fills it in."""
        from pathlib import Path

        from tests_common.test_utils.config import conf_vars

        # Simulate an older Airflow version by temporarily hiding _log from
        # the core class; the compat class must still expose it.
        with mock.patch.object(CoreBaseDagBundle, "_log", None, create=True):
            from importlib import reload

            import airflow.providers.common.compat.bundles as mod

            reload(mod)
            CompatBase = mod.BaseDagBundle

        class _DummyBundle(CompatBase):
            @property
            def path(self) -> Path:
                return tmp_path

            def initialize(self) -> None:
                pass

            def get_current_version(self):
                return None

            def refresh(self) -> None:
                pass

        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path)}):
            bundle = _DummyBundle(name="legacy-bundle")

        assert bundle._log is not None
