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
"""
Airflow compatibility imports for seamless migration from Airflow 2 to Airflow 3.

This module provides lazy imports that automatically try Airflow 3 paths first,
then fall back to Airflow 2 paths, enabling code to work across both versions.
"""

from __future__ import annotations

from typing import Any

# Rename map for classes that changed names between Airflow 2.x and 3.x
# Format: new_name -> (new_path, old_path, old_name)
_RENAME_MAP: dict[str, tuple[str, str, str]] = {
    # Assets: Dataset -> Asset rename in Airflow 3.0
    "Asset": ("airflow.sdk", "airflow.datasets", "Dataset"),
    "AssetAlias": ("airflow.sdk", "airflow.datasets", "DatasetAlias"),
    "AssetAll": ("airflow.sdk", "airflow.datasets", "DatasetAll"),
    "AssetAny": ("airflow.sdk", "airflow.datasets", "DatasetAny"),
}

# Import map for classes/functions/constants
# Format: class_name -> module_path(s)
# - str: single module path (no fallback)
# - tuple[str, ...]: multiple module paths (try in order, newest first)
_IMPORT_MAP: dict[str, str | tuple[str, ...]] = {
    # ============================================================================
    # Hooks
    # ============================================================================
    "BaseHook": ("airflow.sdk", "airflow.hooks.base"),
    "FSHook": ("airflow.providers.standard.hooks.filesystem", "airflow.hooks.filesystem"),
    "SubprocessHook": ("airflow.providers.standard.hooks.subprocess", "airflow.hooks.subprocess"),
    "PackageIndexHook": (
        "airflow.providers.standard.hooks.package_index",
        "airflow.hooks.package_index",
    ),
    # ============================================================================
    # Sensors
    # ============================================================================
    "BaseSensorOperator": ("airflow.sdk", "airflow.sensors.base"),
    "PokeReturnValue": ("airflow.sdk", "airflow.sensors.base"),
    "poke_mode_only": ("airflow.sdk.bases.sensor", "airflow.sensors.base"),
    "PythonSensor": ("airflow.providers.standard.sensors.python", "airflow.sensors.python"),
    "BashSensor": ("airflow.providers.standard.sensors.bash", "airflow.sensors.bash"),
    "DateTimeSensor": ("airflow.providers.standard.sensors.date_time", "airflow.sensors.date_time"),
    "DateTimeSensorAsync": ("airflow.providers.standard.sensors.date_time", "airflow.sensors.date_time"),
    "TimeSensor": ("airflow.providers.standard.sensors.time", "airflow.sensors.time_sensor"),
    "TimeSensorAsync": ("airflow.providers.standard.sensors.time", "airflow.sensors.time_sensor"),
    "TimeDeltaSensor": ("airflow.providers.standard.sensors.time_delta", "airflow.sensors.time_delta"),
    "TimeDeltaSensorAsync": (
        "airflow.providers.standard.sensors.time_delta",
        "airflow.sensors.time_delta",
    ),
    "FileSensor": ("airflow.providers.standard.sensors.filesystem", "airflow.sensors.filesystem"),
    "ExternalTaskSensor": (
        "airflow.providers.standard.sensors.external_task",
        "airflow.sensors.external_task",
    ),
    "ExternalTaskMarker": (
        "airflow.providers.standard.sensors.external_task",
        "airflow.sensors.external_task",
    ),
    "ExternalDagLink": ("airflow.providers.standard.sensors.external_task", "airflow.sensors.external_task"),
    "DayOfWeekSensor": ("airflow.providers.standard.sensors.weekday", "airflow.sensors.weekday"),
    # ============================================================================
    # Operators
    # ============================================================================
    "BaseOperator": ("airflow.sdk", "airflow.models.baseoperator"),
    "PythonOperator": ("airflow.providers.standard.operators.python", "airflow.operators.python"),
    "BranchPythonOperator": ("airflow.providers.standard.operators.python", "airflow.operators.python"),
    "ShortCircuitOperator": ("airflow.providers.standard.operators.python", "airflow.operators.python"),
    "_SERIALIZERS": ("airflow.providers.standard.operators.python", "airflow.operators.python"),
    "PythonVirtualenvOperator": ("airflow.providers.standard.operators.python", "airflow.operators.python"),
    "ExternalPythonOperator": ("airflow.providers.standard.operators.python", "airflow.operators.python"),
    "BranchExternalPythonOperator": (
        "airflow.providers.standard.operators.python",
        "airflow.operators.python",
    ),
    "BranchPythonVirtualenvOperator": (
        "airflow.providers.standard.operators.python",
        "airflow.operators.python",
    ),
    "BashOperator": ("airflow.providers.standard.operators.bash", "airflow.operators.bash"),
    "EmptyOperator": ("airflow.providers.standard.operators.empty", "airflow.operators.empty"),
    "LatestOnlyOperator": (
        "airflow.providers.standard.operators.latest_only",
        "airflow.operators.latest_only",
    ),
    "TriggerDagRunOperator": (
        "airflow.providers.standard.operators.trigger_dagrun",
        "airflow.operators.trigger_dagrun",
    ),
    "BranchDateTimeOperator": ("airflow.providers.standard.operators.datetime", "airflow.operators.datetime"),
    "BranchDayOfWeekOperator": ("airflow.providers.standard.operators.weekday", "airflow.operators.weekday"),
    "BranchMixIn": ("airflow.providers.standard.operators.branch", "airflow.operators.branch"),
    "BaseBranchOperator": ("airflow.providers.standard.operators.branch", "airflow.operators.branch"),
    "SmoothOperator": ("airflow.providers.standard.operators.smooth", "airflow.operators.smooth"),
    # ============================================================================
    # Decorators
    # ============================================================================
    "task": ("airflow.sdk", "airflow.decorators"),
    "dag": ("airflow.sdk", "airflow.decorators"),
    "task_group": ("airflow.sdk", "airflow.decorators"),
    "setup": ("airflow.sdk", "airflow.decorators"),
    "teardown": ("airflow.sdk", "airflow.decorators"),
    "TaskDecorator": ("airflow.sdk.bases.decorator", "airflow.decorators"),
    # ============================================================================
    # Triggers
    # ============================================================================
    "TimeDeltaTrigger": ("airflow.providers.standard.triggers.temporal", "airflow.triggers.temporal"),
    # ============================================================================
    # Models
    # ============================================================================
    "Connection": ("airflow.sdk", "airflow.models.connection"),
    "Variable": ("airflow.sdk", "airflow.models.variable"),
    "XCom": ("airflow.sdk.execution_time.xcom", "airflow.models.xcom"),
    "DAG": ("airflow.sdk", "airflow.models.dag"),
    "DagRun": "airflow.models.dagrun",
    "TaskInstance": "airflow.models.taskinstance",
    "Param": ("airflow.sdk", "airflow.models.param"),
    "XComArg": ("airflow.sdk", "airflow.models.xcom_arg"),
    "MappedOperator": "airflow.models.mappedoperator",
    "DecoratedOperator": ("airflow.sdk.bases.decorator", "airflow.decorators.base"),
    "DecoratedMappedOperator": ("airflow.sdk.bases.decorator", "airflow.decorators.base"),
    # ============================================================================
    # Exceptions
    # ============================================================================
    "AirflowException": "airflow.exceptions",
    "AirflowSkipException": "airflow.exceptions",
    "AirflowFailException": "airflow.exceptions",
    "AirflowSensorTimeout": "airflow.exceptions",
    "AirflowTaskTimeout": "airflow.exceptions",
    "AirflowTaskTerminated": "airflow.exceptions",
    "AirflowNotFoundException": "airflow.exceptions",
    "AirflowConfigException": "airflow.exceptions",
    "AirflowBadRequest": "airflow.exceptions",
    # ============================================================================
    # Assets (Dataset → Asset rename in Airflow 3.0)
    # ============================================================================
    # Note: Asset, AssetAlias, AssetAll, AssetAny are handled by _RENAME_MAP
    # Metadata moved from airflow.datasets.metadata (2.x) to airflow.sdk (3.x)
    "Metadata": ("airflow.sdk", "airflow.datasets.metadata"),
    # ============================================================================
    # Notifiers
    # ============================================================================
    "BaseNotifier": ("airflow.sdk", "airflow.notifications.basenotifier"),
    # ============================================================================
    # Operator Links & Task Groups
    # ============================================================================
    "BaseOperatorLink": ("airflow.sdk", "airflow.models.baseoperatorlink"),
    "TaskGroup": ("airflow.sdk", "airflow.utils.task_group"),
    # ============================================================================
    # Operator Utilities (chain, cross_downstream, etc.)
    # ============================================================================
    "chain": ("airflow.sdk", "airflow.models.baseoperator"),
    "chain_linear": ("airflow.sdk", "airflow.models.baseoperator"),
    "cross_downstream": ("airflow.sdk", "airflow.models.baseoperator"),
    # ============================================================================
    # Edge Modifiers & Labels
    # ============================================================================
    "EdgeModifier": ("airflow.sdk", "airflow.utils.edgemodifier"),
    "Label": ("airflow.sdk", "airflow.utils.edgemodifier"),
    # ============================================================================
    # State Enums
    # ============================================================================
    "DagRunState": ("airflow.sdk", "airflow.utils.state"),
    "TaskInstanceState": ("airflow.sdk", "airflow.utils.state"),
    "TriggerRule": ("airflow.sdk", "airflow.utils.trigger_rule"),
    "WeightRule": ("airflow.sdk", "airflow.utils.weight_rule"),
    # ============================================================================
    # IO & Storage
    # ============================================================================
    "ObjectStoragePath": ("airflow.sdk", "airflow.io.path"),
    # ============================================================================
    # Template Utilities
    # ============================================================================
    "literal": ("airflow.sdk.definitions.template", "airflow.utils.template"),
    # ============================================================================
    # Context & Utilities
    # ============================================================================
    "Context": ("airflow.sdk", "airflow.utils.context"),
    "get_current_context": ("airflow.sdk", "airflow.operators.python"),
    "get_parsing_context": ("airflow.sdk", "airflow.utils.dag_parsing_context"),
    # ============================================================================
    # Python Virtualenv Utilities
    # ============================================================================
    "prepare_virtualenv": (
        "airflow.providers.standard.utils.python_virtualenv",
        "airflow.utils.python_virtualenv",
    ),
    "write_python_script": (
        "airflow.providers.standard.utils.python_virtualenv",
        "airflow.utils.python_virtualenv",
    ),
    # ============================================================================
    # XCom & Task Communication
    # ============================================================================
    "XCOM_RETURN_KEY": "airflow.models.xcom",
}

# Module map: module_name -> module_path(s)
# For entire modules that have been moved (e.g., timezone)
# Usage: from airflow.providers.common.compat.lazy_compat import timezone
_MODULE_MAP: dict[str, str | tuple[str, ...]] = {
    "timezone": ("airflow.sdk.timezone", "airflow.utils.timezone"),
    "io": ("airflow.sdk.io", "airflow.io"),
}


def __getattr__(name: str) -> Any:
    """
    Lazy import compatibility layer.

    Tries to import from Airflow 3 paths first, falls back to Airflow 2 paths.
    This enables code to work across both Airflow 2.x and 3.x versions.

    Supports:
    - Renamed classes from _RENAME_MAP: classes that changed names (e.g., Dataset -> Asset)
    - Attributes from _IMPORT_MAP: classes, functions, constants
    - Modules from _MODULE_MAP: entire modules that have moved

    :param name: Name of the class/function/module to import
    :return: The imported class/function/module
    :raises AttributeError: If the name is not in any map
    :raises ImportError: If all import paths fail
    """
    # Check if this is a renamed class
    if name in _RENAME_MAP:
        new_path, old_path, old_name = _RENAME_MAP[name]

        rename_error: ImportError | ModuleNotFoundError | AttributeError | None = None
        # Try new path with new name first (Airflow 3.x)
        try:
            module = __import__(new_path, fromlist=[name])
            return getattr(module, name)
        except (ImportError, ModuleNotFoundError, AttributeError) as e:
            rename_error = e

        # Fall back to old path with old name (Airflow 2.x)
        try:
            module = __import__(old_path, fromlist=[old_name])
            return getattr(module, old_name)
        except (ImportError, ModuleNotFoundError, AttributeError):
            if rename_error:
                raise ImportError(
                    f"Could not import {name!r} from {new_path!r} or {old_name!r} from {old_path!r}"
                ) from rename_error
            raise

    # Check if this is a module import
    if name in _MODULE_MAP:
        import importlib

        paths = _MODULE_MAP[name]
        if isinstance(paths, str):
            paths = (paths,)

        module_error: ImportError | ModuleNotFoundError | None = None
        for module_path in paths:
            try:
                return importlib.import_module(module_path)
            except (ImportError, ModuleNotFoundError) as e:
                module_error = e
                continue

        if module_error:
            raise ImportError(f"Could not import module {name!r} from any of: {paths}") from module_error

    # Check if this is an attribute import
    if name in _IMPORT_MAP:
        paths = _IMPORT_MAP[name]
        if isinstance(paths, str):
            paths = (paths,)

        attr_error: ImportError | ModuleNotFoundError | AttributeError | None = None
        for module_path in paths:
            try:
                module = __import__(module_path, fromlist=[name])
                return getattr(module, name)
            except (ImportError, ModuleNotFoundError, AttributeError) as e:
                attr_error = e
                continue

        if attr_error:
            raise ImportError(f"Could not import {name!r} from any of: {paths}") from attr_error

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = list(_RENAME_MAP.keys()) + list(_IMPORT_MAP.keys()) + list(_MODULE_MAP.keys())
