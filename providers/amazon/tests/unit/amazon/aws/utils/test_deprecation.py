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

import importlib.util
import re
import sys
import warnings
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING
from unittest import mock

import pytest

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.amazon.aws.exceptions import RemovedFeatureError
from airflow.providers.amazon.aws.utils import deprecation
from airflow.providers.amazon.aws.utils.deprecation import (
    NEXT_MAJOR_VERSION,
    UPGRADE_GUIDE_URL,
    is_next_major_enabled,
    warn_deprecated,
)
from airflow.providers.common.compat.sdk import AirflowException

from tests_common.test_utils.version_compat import AIRFLOW_V_3_2_PLUS

if AIRFLOW_V_3_2_PLUS:
    from airflow.dag_processing.dagbag import DagBag
else:
    from airflow.models.dagbag import DagBag  # type: ignore[no-redef, attribute-defined]

if TYPE_CHECKING:
    from pathlib import Path
    from types import ModuleType

PROVIDER_MODULE = "airflow.providers.amazon.aws.deprecation_probe"
PROVIDER_SOURCE = """
from airflow.providers.amazon.aws.utils.deprecation import NEXT_MAJOR_VERSION, warn_deprecated
from airflow.providers.common.compat.sdk import BaseOperator


class ProbeOperator(BaseOperator):
    def __init__(self, *, old=None, **kwargs):
        super().__init__(**kwargs)
        if old is not None:
            warn_deprecated("`old` is deprecated. Use `new` instead.", removal_version=NEXT_MAJOR_VERSION)


class ProbeSubclassOperator(ProbeOperator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


def probe_hook_method():
    warn_deprecated("`probe_hook_method` is deprecated.", removal_version=NEXT_MAJOR_VERSION)
"""

DAG_SOURCE = f"""
from {PROVIDER_MODULE} import ProbeOperator
from airflow.providers.common.compat.sdk import DAG

with DAG("deprecation_probe", schedule=None):
    ProbeOperator(task_id="probe", old="value")
"""
DAG_OPERATOR_LINE = DAG_SOURCE.splitlines().index('    ProbeOperator(task_id="probe", old="value")') + 1

CALLER_SOURCE = """
def call(deprecated, **kwargs):
    return deprecated(**kwargs)


def call_twice(deprecated, **kwargs):
    for _ in range(2):
        deprecated(**kwargs)
"""
CALL_LINE = CALLER_SOURCE.splitlines().index("    return deprecated(**kwargs)") + 1

OPERATOR_KWARGS = {"task_id": "probe", "old": "value"}


def build_message(message: str, *, removal: str, version: str = NEXT_MAJOR_VERSION) -> str:
    return (
        f"{message} The deprecated behaviour {removal} in version {version} of the Amazon provider. "
        f"See {UPGRADE_GUIDE_URL}"
    )


def load_module(name: str, path: Path, source: str) -> ModuleType:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(source)
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec
    assert spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(autouse=True)
def disable_next_major(monkeypatch):
    """Pin the switch off, so the tests of the warning also pass once the switch is turned on."""
    monkeypatch.setattr(deprecation, "_NEXT_MAJOR_ENABLED", False)


@pytest.fixture
def provider(tmp_path) -> ModuleType:
    return load_module(PROVIDER_MODULE, tmp_path / "deprecation_probe.py", PROVIDER_SOURCE)


@contextmanager
def airflow_default_filters() -> Iterator[list[warnings.WarningMessage]]:
    with warnings.catch_warnings(record=True) as recorded:
        warnings.resetwarnings()
        # Python ignores DeprecationWarning outside ``__main__``, and Airflow turns it back on for
        # ``airflow`` modules only.
        warnings.simplefilter("ignore", DeprecationWarning)
        warnings.filterwarnings("default", category=DeprecationWarning, module="airflow")
        yield recorded


@pytest.mark.parametrize("enabled", [True, False])
def test_is_next_major_enabled_reads_the_switch_when_called(monkeypatch, enabled):
    monkeypatch.setattr(deprecation, "_NEXT_MAJOR_ENABLED", enabled)

    assert is_next_major_enabled() is enabled


def test_warn_deprecated_names_removal_release_and_upgrade_guide():
    with pytest.warns(AirflowProviderDeprecationWarning) as recorded:
        warn_deprecated("`old` is deprecated. Use `new` instead.", removal_version=NEXT_MAJOR_VERSION)

    assert [str(w.message) for w in recorded] == [
        build_message("`old` is deprecated. Use `new` instead.", removal="will be removed")
    ]


@mock.patch.object(deprecation, "_NEXT_MAJOR_ENABLED", new=True)
@pytest.mark.parametrize("caught_as", [RemovedFeatureError, AirflowException, TypeError])
def test_warn_deprecated_raises_for_the_next_major_once_it_is_enabled(caught_as):
    expected = build_message("`old` is deprecated. Use `new` instead.", removal="was removed")

    with pytest.raises(caught_as, match=re.escape(expected)):
        warn_deprecated("`old` is deprecated. Use `new` instead.", removal_version=NEXT_MAJOR_VERSION)


@mock.patch.object(deprecation, "_NEXT_MAJOR_ENABLED", new=True)
def test_warn_deprecated_keeps_warning_for_a_later_release_once_next_major_is_enabled():
    with pytest.warns(AirflowProviderDeprecationWarning) as recorded:
        warn_deprecated("`old` is deprecated. Use `new` instead.", removal_version="999.0.0")

    assert [str(w.message) for w in recorded] == [
        build_message("`old` is deprecated. Use `new` instead.", removal="will be removed", version="999.0.0")
    ]


def test_operator_warning_reaches_the_dag_importer_at_the_dag_file_line(tmp_path, monkeypatch, provider):
    monkeypatch.setitem(sys.modules, PROVIDER_MODULE, provider)
    dag_file = tmp_path / "dags" / "my_dag.py"
    dag_file.parent.mkdir()
    dag_file.write_text(DAG_SOURCE)
    dagbag = DagBag(dag_folder=str(dag_file.parent), collect_dags=False)

    with airflow_default_filters():
        dagbag.process_file(str(dag_file), only_if_updated=False)

    assert dagbag.import_errors == {}
    assert [warning for captured in dagbag.captured_warnings.values() for warning in captured] == [
        f"{dag_file}:{DAG_OPERATOR_LINE}: airflow.exceptions.AirflowProviderDeprecationWarning: "
        + build_message("`old` is deprecated. Use `new` instead.", removal="will be removed")
    ]


@pytest.mark.parametrize(
    ("deprecated", "kwargs"),
    [
        pytest.param("ProbeSubclassOperator", OPERATOR_KWARGS, id="operator_subclass_param"),
        pytest.param("probe_hook_method", {}, id="hook_method"),
    ],
)
def test_warning_triggered_by_user_code_is_shown_at_the_user_line(tmp_path, provider, deprecated, kwargs):
    # Named the way the Dag importer names a Dag file's module.
    user_code = load_module("unusual_prefix_0123_my_dag", tmp_path / "my_dag.py", CALLER_SOURCE)

    with airflow_default_filters() as recorded:
        user_code.call(getattr(provider, deprecated), **kwargs)

    assert [(w.category, w.filename, w.lineno) for w in recorded] == [
        (AirflowProviderDeprecationWarning, user_code.__file__, CALL_LINE)
    ]


@pytest.mark.parametrize(
    ("caller_name", "caller_path", "deprecated", "kwargs"),
    [
        pytest.param("airflow.probe_task_runner", "runner.py", "probe_hook_method", {}, id="airflow"),
        pytest.param(
            "airflow.probe_mapped_operator", "mapped.py", "ProbeOperator", OPERATOR_KWARGS, id="airflow_init"
        ),
        pytest.param("functools", "functools.py", "probe_hook_method", {}, id="standard_library"),
        pytest.param(
            "probe_retry_library",
            "site-packages/probe_retry_library.py",
            "probe_hook_method",
            {},
            id="installed_library",
        ),
    ],
)
def test_warning_not_triggered_by_user_code_is_shown_at_the_provider_code(
    tmp_path, provider, caller_name, caller_path, deprecated, kwargs
):
    caller = load_module(caller_name, tmp_path / caller_path, CALLER_SOURCE)

    with airflow_default_filters() as recorded:
        caller.call(getattr(provider, deprecated), **kwargs)

    assert [(w.category, w.filename) for w in recorded] == [
        (AirflowProviderDeprecationWarning, provider.__file__)
    ]


def test_warning_repeated_from_the_same_line_is_shown_once(tmp_path, provider):
    user_code = load_module("unusual_prefix_0123_my_dag", tmp_path / "my_dag.py", CALLER_SOURCE)

    with airflow_default_filters() as recorded:
        user_code.call_twice(provider.probe_hook_method)

    assert len(recorded) == 1
