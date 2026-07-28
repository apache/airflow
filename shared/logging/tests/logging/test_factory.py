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

from types import SimpleNamespace
from typing import Any

import pytest

from airflow_shared.logging.factory import (
    DEFAULT_LOGGING_CONFIG_PATH,
    _build_remote_task_log_from_provider,
    resolve_remote_task_log,
)


class FakeProvidersManager:
    """Stub manager satisfying the ProvidersManagerLike protocol."""

    def __init__(self, by_scheme: dict[str, Any] | None = None) -> None:
        self._by_scheme = by_scheme or {}

    def remote_logging_handler_by_scheme(self, scheme: str):
        return self._by_scheme.get(scheme)


class ExplodingProvidersManager:
    """Stub manager whose lookup must never be called by the test."""

    def remote_logging_handler_by_scheme(self, scheme: str):  # pragma: no cover - sentinel
        pytest.fail("lookup must not run")


class FakeRemoteLogIO:
    """Importable stub used as the registered handler class."""

    last_called: bool = False

    def __init__(self):
        pass

    @classmethod
    def from_config(cls):
        FakeRemoteLogIO.last_called = True
        return cls()


class FakeRemoteLogIONoFactory:
    """Stub missing the from_config classmethod."""


_LOOKUP: dict[str, Any] = {
    "fakeremotelogio": FakeRemoteLogIO,
    "fakeremotelogionofactory": FakeRemoteLogIONoFactory,
}


def _fake_import_string(path: str) -> Any:
    # Tests pass simple names like "fakeremotelogio"; route them through a dict
    # to avoid needing real importable modules.
    return _LOOKUP[path.lower()]


def _info(
    *,
    classpath: str = "fakeremotelogio",
    scheme: str = "fake",
    package_name: str = "fake-provider",
) -> SimpleNamespace:
    return SimpleNamespace(
        classpath=classpath,
        scheme=scheme,
        package_name=package_name,
    )


@pytest.fixture(autouse=True)
def _reset_fake():
    FakeRemoteLogIO.last_called = False
    yield
    FakeRemoteLogIO.last_called = False


def test_provider_dispatch_resolves_by_scheme():
    info = _info(scheme="fake")
    handler = _build_remote_task_log_from_provider(
        remote_base_log_folder="fake://bucket/path",
        providers_manager=FakeProvidersManager({"fake": info}),
        import_string=_fake_import_string,
    )
    assert isinstance(handler, FakeRemoteLogIO)
    assert FakeRemoteLogIO.last_called is True


def test_provider_dispatch_returns_none_when_scheme_unknown():
    handler = _build_remote_task_log_from_provider(
        remote_base_log_folder="unknown://x",
        providers_manager=FakeProvidersManager(),
        import_string=_fake_import_string,
    )
    assert handler is None


def test_provider_dispatch_returns_none_when_remote_base_unset():
    handler = _build_remote_task_log_from_provider(
        remote_base_log_folder=None,
        providers_manager=ExplodingProvidersManager(),
        import_string=_fake_import_string,
    )
    assert handler is None


def test_provider_dispatch_returns_none_when_no_scheme_in_url():
    handler = _build_remote_task_log_from_provider(
        remote_base_log_folder="/local/path/no/scheme",
        providers_manager=ExplodingProvidersManager(),
        import_string=_fake_import_string,
    )
    assert handler is None


def test_provider_dispatch_skips_handler_without_factory():
    info = _info(classpath="fakeremotelogionofactory")
    handler = _build_remote_task_log_from_provider(
        remote_base_log_folder="fake://b",
        providers_manager=FakeProvidersManager({"fake": info}),
        import_string=_fake_import_string,
    )
    assert handler is None


# ---------------------------------------------------------------------------
# Tests for resolve_remote_task_log (three-tier precedence orchestrator)
# ---------------------------------------------------------------------------

_FAKE_LOGGING_DICT: dict[str, Any] = {"version": 1}


class FakeConf:
    """Minimal ConfLike stub keyed by ``(section, key)``."""

    def __init__(self, values: dict[tuple[str, str], Any] | None = None) -> None:
        self._values = values or {}

    def get(self, section, key, **kwargs):
        return self._values.get((section, key), kwargs.get("fallback"))

    def getboolean(self, section, key, **kwargs):
        value = self._values.get((section, key), kwargs.get("fallback"))
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() in ("1", "true", "yes", "on")
        return bool(value)


def test_resolve_user_defined_module_wins():
    """If the user's logging_config_class module exposes REMOTE_TASK_LOG, it wins."""
    user_path = "fake.user.module.DEFAULT_LOGGING_CONFIG"
    sentinel_remote = object()

    def fake_import_string(path: str):
        if path == user_path:
            return _FAKE_LOGGING_DICT
        if path == "fake.user.module":
            return SimpleNamespace(REMOTE_TASK_LOG=sentinel_remote, DEFAULT_REMOTE_CONN_ID="user_conn")
        raise AssertionError(f"unexpected import: {path}")

    import sys

    sys.modules["fake.user.module"] = SimpleNamespace(
        REMOTE_TASK_LOG=sentinel_remote, DEFAULT_REMOTE_CONN_ID="user_conn"
    )
    try:
        handler, conn_id = resolve_remote_task_log(
            conf=FakeConf({("logging", "logging_config_class"): user_path}),
            providers_manager=ExplodingProvidersManager(),  # must not be consulted
            import_string=fake_import_string,
        )
    finally:
        sys.modules.pop("fake.user.module", None)

    assert handler is sentinel_remote
    assert conn_id == "user_conn"


def test_resolve_falls_through_to_provider_dispatch():
    """Default logging_class_path → user_defined is False → provider dispatch runs."""
    info = _info(scheme="fake")
    handler, conn_id = resolve_remote_task_log(
        conf=FakeConf(
            {
                ("logging", "remote_base_log_folder"): "fake://bucket",
                ("logging", "remote_logging"): True,
            }
        ),
        providers_manager=FakeProvidersManager({"fake": info}),
        import_string=_fake_import_string,
    )
    assert isinstance(handler, FakeRemoteLogIO)
    assert conn_id is None


def test_resolve_skips_provider_dispatch_when_remote_logging_disabled():
    """``[logging] remote_logging = False`` must short-circuit provider dispatch."""
    sentinel_remote = object()

    def fake_import_string(path: str):
        if path == DEFAULT_LOGGING_CONFIG_PATH:
            return _FAKE_LOGGING_DICT
        raise AssertionError(f"unexpected import: {path}")

    import sys

    parent_module = DEFAULT_LOGGING_CONFIG_PATH.rsplit(".", 1)[0]
    saved = sys.modules.get(parent_module)
    sys.modules[parent_module] = SimpleNamespace(REMOTE_TASK_LOG=sentinel_remote, DEFAULT_REMOTE_CONN_ID=None)
    try:
        # ExplodingProvidersManager fails the test if provider dispatch runs.
        handler, conn_id = resolve_remote_task_log(
            conf=FakeConf(
                {
                    ("logging", "remote_base_log_folder"): "fake://bucket",
                    ("logging", "remote_logging"): False,
                }
            ),
            providers_manager=ExplodingProvidersManager(),
            import_string=fake_import_string,
        )
    finally:
        if saved is not None:
            sys.modules[parent_module] = saved
        else:
            sys.modules.pop(parent_module, None)

    assert handler is sentinel_remote
    assert conn_id is None


def test_resolve_falls_back_to_legacy_attr_path_when_dispatch_returns_none():
    """When provider dispatch yields nothing, the legacy module-attr path is consulted."""
    sentinel_remote = object()

    def fake_import_string(path: str):
        if path == DEFAULT_LOGGING_CONFIG_PATH:
            return _FAKE_LOGGING_DICT
        raise AssertionError(f"unexpected import: {path}")

    import sys

    parent_module = DEFAULT_LOGGING_CONFIG_PATH.rsplit(".", 1)[0]
    saved = sys.modules.get(parent_module)
    sys.modules[parent_module] = SimpleNamespace(
        REMOTE_TASK_LOG=sentinel_remote, DEFAULT_REMOTE_CONN_ID="legacy_conn"
    )
    try:
        handler, conn_id = resolve_remote_task_log(
            conf=FakeConf(
                {
                    ("logging", "remote_base_log_folder"): "unknown://b",
                    ("logging", "remote_logging"): True,
                }
            ),
            providers_manager=FakeProvidersManager(),  # empty registry
            import_string=fake_import_string,
        )
    finally:
        if saved is not None:
            sys.modules[parent_module] = saved
        else:
            sys.modules.pop(parent_module, None)

    assert handler is sentinel_remote
    assert conn_id == "legacy_conn"
