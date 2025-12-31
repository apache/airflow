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

from typing import TYPE_CHECKING

from airflow.providers.common.compat.version_compat import AIRFLOW_V_3_0_PLUS

if TYPE_CHECKING:
    import airflow.sdk.io as io  # noqa: F401
    import airflow.sdk.timezone as timezone  # noqa: F401
    from airflow.models.xcom import XCOM_RETURN_KEY as XCOM_RETURN_KEY
    from airflow.sdk import (
        DAG as DAG,
        Asset as Asset,
        AssetAlias as AssetAlias,
        AssetAll as AssetAll,
        AssetAny as AssetAny,
        BaseHook as BaseHook,
        BaseNotifier as BaseNotifier,
        BaseOperator as BaseOperator,
        BaseOperatorLink as BaseOperatorLink,
        BaseSensorOperator as BaseSensorOperator,
        Connection as Connection,
        Context as Context,
        DagRunState as DagRunState,
        EdgeModifier as EdgeModifier,
        Label as Label,
        Metadata as Metadata,
        ObjectStoragePath as ObjectStoragePath,
        Param as Param,
        PokeReturnValue as PokeReturnValue,
        TaskGroup as TaskGroup,
        TaskInstanceState as TaskInstanceState,
        TriggerRule as TriggerRule,
        Variable as Variable,
        WeightRule as WeightRule,
        XComArg as XComArg,
        chain as chain,
        chain_linear as chain_linear,
        cross_downstream as cross_downstream,
        dag as dag,
        get_current_context as get_current_context,
        get_parsing_context as get_parsing_context,
        setup as setup,
        task as task,
        task_group as task_group,
        teardown as teardown,
    )
    from airflow.sdk.bases.decorator import (
        DecoratedMappedOperator as DecoratedMappedOperator,
        DecoratedOperator as DecoratedOperator,
        TaskDecorator as TaskDecorator,
        get_unique_task_id as get_unique_task_id,
        task_decorator_factory as task_decorator_factory,
    )
    from airflow.sdk.bases.sensor import poke_mode_only as poke_mode_only
    from airflow.sdk.definitions.context import context_merge as context_merge
    from airflow.sdk.definitions.mappedoperator import MappedOperator as MappedOperator
    from airflow.sdk.definitions.template import literal as literal
    from airflow.sdk.exceptions import (
        AirflowException as AirflowException,
        AirflowFailException as AirflowFailException,
        AirflowNotFoundException as AirflowNotFoundException,
        AirflowSensorTimeout as AirflowSensorTimeout,
        AirflowSkipException as AirflowSkipException,
        AirflowTaskTimeout as AirflowTaskTimeout,
        ParamValidationError as ParamValidationError,
        TaskDeferred as TaskDeferred,
        XComNotFound as XComNotFound,
    )
    from airflow.sdk.log import redact as redact
    from airflow.sdk.observability.stats import Stats as Stats
    from airflow.sdk.plugins_manager import AirflowPlugin as AirflowPlugin

    # Airflow 3-only exceptions (conditionally imported)
    if AIRFLOW_V_3_0_PLUS:
        from airflow.sdk.exceptions import (
            DagRunTriggerException as DagRunTriggerException,
            DownstreamTasksSkipped as DownstreamTasksSkipped,
        )
    from airflow.sdk.execution_time.context import (
        AIRFLOW_VAR_NAME_FORMAT_MAPPING as AIRFLOW_VAR_NAME_FORMAT_MAPPING,
        context_to_airflow_vars as context_to_airflow_vars,
    )
    from airflow.sdk.execution_time.timeout import timeout as timeout
    from airflow.sdk.execution_time.xcom import XCom as XCom


from airflow.providers.common.compat._compat_utils import create_module_getattr

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
    # ============================================================================
    # Sensors
    # ============================================================================
    "BaseSensorOperator": ("airflow.sdk", "airflow.sensors.base"),
    "PokeReturnValue": ("airflow.sdk", "airflow.sensors.base"),
    "poke_mode_only": ("airflow.sdk.bases.sensor", "airflow.sensors.base"),
    # ============================================================================
    # Operators
    # ============================================================================
    "BaseOperator": ("airflow.sdk", "airflow.models.baseoperator"),
    # ============================================================================
    # Decorators
    # ============================================================================
    "task": ("airflow.sdk", "airflow.decorators"),
    "dag": ("airflow.sdk", "airflow.decorators"),
    "task_group": ("airflow.sdk", "airflow.decorators"),
    "setup": ("airflow.sdk", "airflow.decorators"),
    "teardown": ("airflow.sdk", "airflow.decorators"),
    "TaskDecorator": ("airflow.sdk.bases.decorator", "airflow.decorators"),
    "task_decorator_factory": ("airflow.sdk.bases.decorator", "airflow.decorators.base"),
    "get_unique_task_id": ("airflow.sdk.bases.decorator", "airflow.decorators.base"),
    # ============================================================================
    # Models
    # ============================================================================
    "Connection": ("airflow.sdk", "airflow.models.connection"),
    "Variable": ("airflow.sdk", "airflow.models.variable"),
    "XCom": ("airflow.sdk.execution_time.xcom", "airflow.models.xcom"),
    "DAG": ("airflow.sdk", "airflow.models.dag"),
    "Param": ("airflow.sdk", "airflow.models.param"),
    "XComArg": ("airflow.sdk", "airflow.models.xcom_arg"),
    "DecoratedOperator": ("airflow.sdk.bases.decorator", "airflow.decorators.base"),
    "DecoratedMappedOperator": ("airflow.sdk.bases.decorator", "airflow.decorators.base"),
    "MappedOperator": ("airflow.sdk.definitions.mappedoperator", "airflow.models.mappedoperator"),
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
    # Plugins
    # ============================================================================
    "AirflowPlugin": ("airflow.sdk.plugins_manager", "airflow.plugins_manager"),
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
    "context_merge": ("airflow.sdk.definitions.context", "airflow.utils.context"),
    "context_to_airflow_vars": ("airflow.sdk.execution_time.context", "airflow.utils.operator_helpers"),
    "AIRFLOW_VAR_NAME_FORMAT_MAPPING": (
        "airflow.sdk.execution_time.context",
        "airflow.utils.operator_helpers",
    ),
    "get_current_context": ("airflow.sdk", "airflow.operators.python"),
    "get_parsing_context": ("airflow.sdk", "airflow.utils.dag_parsing_context"),
    # ============================================================================
    # Timeout Utilities
    # ============================================================================
    "timeout": ("airflow.sdk.execution_time.timeout", "airflow.utils.timeout"),
    # ============================================================================
    # XCom & Task Communication
    # ============================================================================
    "XCOM_RETURN_KEY": "airflow.models.xcom",
    # ============================================================================
    # Exceptions (deprecated in airflow.exceptions, prefer SDK)
    # ============================================================================
    # Note: AirflowException and AirflowNotFoundException are not deprecated, but exposing them
    # here keeps provider imports consistent across Airflow 2 and 3.
    "AirflowException": ("airflow.sdk.exceptions", "airflow.exceptions"),
    "AirflowFailException": ("airflow.sdk.exceptions", "airflow.exceptions"),
    "AirflowNotFoundException": ("airflow.sdk.exceptions", "airflow.exceptions"),
    "AirflowSkipException": ("airflow.sdk.exceptions", "airflow.exceptions"),
    "AirflowTaskTimeout": ("airflow.sdk.exceptions", "airflow.exceptions"),
    "AirflowSensorTimeout": ("airflow.sdk.exceptions", "airflow.exceptions"),
    "ParamValidationError": ("airflow.sdk.exceptions", "airflow.exceptions"),
    "TaskDeferred": ("airflow.sdk.exceptions", "airflow.exceptions"),
    "XComNotFound": ("airflow.sdk.exceptions", "airflow.exceptions"),
    # ============================================================================
    # Observability
    # ============================================================================
    "Stats": ("airflow.sdk.observability.stats", "airflow.stats"),
    # ============================================================================
    # Secrets Masking
    # ============================================================================
    "redact": (
        "airflow.sdk.log",
        "airflow.sdk._shared.secrets_masker",
        "airflow.sdk.execution_time.secrets_masker",
        "airflow.utils.log.secrets_masker",
    ),
}

# Airflow 3-only exceptions (not available in Airflow 2)
_AIRFLOW_3_ONLY_EXCEPTIONS: dict[str, tuple[str, ...]] = {
    "DownstreamTasksSkipped": ("airflow.sdk.exceptions", "airflow.exceptions"),
    "DagRunTriggerException": ("airflow.sdk.exceptions", "airflow.exceptions"),
}

# Add Airflow 3-only exceptions to _IMPORT_MAP if running Airflow 3+
if AIRFLOW_V_3_0_PLUS:
    _IMPORT_MAP.update(_AIRFLOW_3_ONLY_EXCEPTIONS)

# Module map: module_name -> module_path(s)
# For entire modules that have been moved (e.g., timezone)
# Usage: from airflow.providers.common.compat.lazy_compat import timezone
_MODULE_MAP: dict[str, str | tuple[str, ...]] = {
    "timezone": ("airflow.sdk.timezone", "airflow.utils.timezone"),
    "io": ("airflow.sdk.io", "airflow.io"),
}

# Use the shared utility to create __getattr__
__getattr__ = create_module_getattr(
    import_map=_IMPORT_MAP,
    module_map=_MODULE_MAP,
    rename_map=_RENAME_MAP,
)

__all__ = list(_RENAME_MAP.keys()) + list(_IMPORT_MAP.keys()) + list(_MODULE_MAP.keys())
