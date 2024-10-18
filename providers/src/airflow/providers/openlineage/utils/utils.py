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

import datetime
import json
import logging
from contextlib import suppress
from functools import wraps
from importlib import metadata
from typing import TYPE_CHECKING, Any, Callable, Iterable

import attrs
from deprecated import deprecated
from openlineage.client.utils import RedactMixin
from packaging.version import Version

from airflow import __version__ as AIRFLOW_VERSION
from airflow.exceptions import (
    AirflowProviderDeprecationWarning,
)

# TODO: move this maybe to Airflow's logic?
from airflow.models import DAG, BaseOperator, DagRun, MappedOperator
from airflow.providers.common.compat.assets import Asset
from airflow.providers.openlineage import conf
from airflow.providers.openlineage.plugins.facets import (
    AirflowDagRunFacet,
    AirflowDebugRunFacet,
    AirflowJobFacet,
    AirflowMappedTaskRunFacet,
    AirflowRunFacet,
    AirflowStateRunFacet,
    UnknownOperatorAttributeRunFacet,
    UnknownOperatorInstance,
)
from airflow.providers.openlineage.utils.selective_enable import (
    is_dag_lineage_enabled,
    is_task_lineage_enabled,
)
from airflow.serialization.serialized_objects import SerializedBaseOperator
from airflow.utils.context import AirflowContextDeprecationWarning
from airflow.utils.log.secrets_masker import (
    Redactable,
    Redacted,
    SecretsMasker,
    should_hide_value_for_key,
)
from airflow.utils.module_loading import import_string

if TYPE_CHECKING:
    from openlineage.client.event_v2 import Dataset as OpenLineageDataset
    from openlineage.client.facet_v2 import RunFacet

    from airflow.models import TaskInstance
    from airflow.utils.state import DagRunState, TaskInstanceState

log = logging.getLogger(__name__)
_NOMINAL_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
IS_AIRFLOW_2_10_OR_HIGHER = Version(Version(AIRFLOW_VERSION).base_version) >= Version("2.10.0")


def try_import_from_string(string: str) -> Any:
    with suppress(ImportError):
        return import_string(string)


def get_operator_class(task: BaseOperator) -> type:
    if task.__class__.__name__ in ("DecoratedMappedOperator", "MappedOperator"):
        return task.operator_class
    return task.__class__


def get_job_name(task: TaskInstance) -> str:
    return f"{task.dag_id}.{task.task_id}"


def get_airflow_mapped_task_facet(task_instance: TaskInstance) -> dict[str, Any]:
    # check for -1 comes from SmartSensor compatibility with dynamic task mapping
    # this comes from Airflow code
    log.debug(
        "AirflowMappedTaskRunFacet is deprecated and will be removed. "
        "Use information from AirflowRunFacet instead."
    )
    if hasattr(task_instance, "map_index") and getattr(task_instance, "map_index") != -1:
        return {"airflow_mappedTask": AirflowMappedTaskRunFacet.from_task_instance(task_instance)}
    return {}


def get_user_provided_run_facets(ti: TaskInstance, ti_state: TaskInstanceState) -> dict[str, RunFacet]:
    custom_facets = {}

    # Append custom run facets by executing the custom_run_facet functions.
    for custom_facet_func in conf.custom_run_facets():
        try:
            func: Callable[[TaskInstance, TaskInstanceState], dict[str, RunFacet]] | None = (
                try_import_from_string(custom_facet_func)
            )
            if not func:
                log.warning(
                    "OpenLineage is unable to import custom facet function `%s`; will ignore it.",
                    custom_facet_func,
                )
                continue
            facets: dict[str, RunFacet] | None = func(ti, ti_state)
            if facets and isinstance(facets, dict):
                duplicate_facet_keys = [facet_key for facet_key in facets if facet_key in custom_facets]
                if duplicate_facet_keys:
                    log.warning(
                        "Duplicate OpenLineage custom facets key(s) found: `%s` from function `%s`; "
                        "this will overwrite the previous value.",
                        ", ".join(duplicate_facet_keys),
                        custom_facet_func,
                    )
                log.debug(
                    "Adding OpenLineage custom facet with key(s): `%s` from function `%s`.",
                    tuple(facets),
                    custom_facet_func,
                )
                custom_facets.update(facets)
        except Exception as exc:
            log.warning(
                "Error processing custom facet function `%s`; will ignore it. Error was: %s: %s",
                custom_facet_func,
                type(exc).__name__,
                exc,
            )
    return custom_facets


def get_fully_qualified_class_name(operator: BaseOperator | MappedOperator) -> str:
    if isinstance(operator, (MappedOperator, SerializedBaseOperator)):
        # as in airflow.api_connexion.schemas.common_schema.ClassReferenceSchema
        return operator._task_module + "." + operator._task_type  # type: ignore
    op_class = get_operator_class(operator)
    return op_class.__module__ + "." + op_class.__name__


def is_operator_disabled(operator: BaseOperator | MappedOperator) -> bool:
    return get_fully_qualified_class_name(operator) in conf.disabled_operators()


def is_selective_lineage_enabled(obj: DAG | BaseOperator | MappedOperator) -> bool:
    """If selective enable is active check if DAG or Task is enabled to emit events."""
    if not conf.selective_enable():
        return True
    if isinstance(obj, DAG):
        return is_dag_lineage_enabled(obj)
    elif isinstance(obj, (BaseOperator, MappedOperator)):
        return is_task_lineage_enabled(obj)
    else:
        raise TypeError("is_selective_lineage_enabled can only be used on DAG or Operator objects")


class InfoJsonEncodable(dict):
    """
    Airflow objects might not be json-encodable overall.

    The class provides additional attributes to control
    what and how is encoded:

    * renames: a dictionary of attribute name changes
    * | casts: a dictionary consisting of attribute names
      | and corresponding methods that should change
      | object value
    * includes: list of attributes to be included in encoding
    * excludes: list of attributes to be excluded from encoding

    Don't use both includes and excludes.
    """

    renames: dict[str, str] = {}
    casts: dict[str, Any] = {}
    includes: list[str] = []
    excludes: list[str] = []

    def __init__(self, obj):
        self.obj = obj
        self._fields = []

        self._cast_fields()
        self._rename_fields()
        self._include_fields()
        dict.__init__(
            self,
            **{field: InfoJsonEncodable._cast_basic_types(getattr(self, field)) for field in self._fields},
        )

    @staticmethod
    def _cast_basic_types(value):
        if isinstance(value, datetime.datetime):
            return value.isoformat()
        if isinstance(value, datetime.timedelta):
            return f"{value.total_seconds()} seconds"
        if isinstance(value, (set, list, tuple)):
            return str(list(value))
        return value

    def _rename_fields(self):
        for field, renamed in self.renames.items():
            if hasattr(self.obj, field):
                setattr(self, renamed, getattr(self.obj, field))
                self._fields.append(renamed)

    def _cast_fields(self):
        for field, func in self.casts.items():
            setattr(self, field, func(self.obj))
            self._fields.append(field)

    def _include_fields(self):
        if self.includes and self.excludes:
            raise ValueError("Don't use both includes and excludes.")
        if self.includes:
            for field in self.includes:
                if field not in self._fields and hasattr(self.obj, field):
                    setattr(self, field, getattr(self.obj, field))
                    self._fields.append(field)
        else:
            if hasattr(self.obj, "__dict__"):
                obj_fields = self.obj.__dict__
            elif attrs.has(self.obj.__class__):  # e.g. attrs.define class with slots=True has no __dict__
                obj_fields = {
                    field.name: getattr(self.obj, field.name) for field in attrs.fields(self.obj.__class__)
                }
            else:
                raise ValueError(
                    "Cannot iterate over fields: "
                    f"The object of type {type(self.obj).__name__} neither has a __dict__ attribute "
                    "nor is defined as an attrs class."
                )
            for field, val in obj_fields.items():
                if field not in self._fields and field not in self.excludes and field not in self.renames:
                    setattr(self, field, val)
                    self._fields.append(field)


class DagInfo(InfoJsonEncodable):
    """Defines encoding DAG object to JSON."""

    includes = [
        "dag_id",
        "description",
        "fileloc",
        "owner",
        "schedule_interval",  # For Airflow 2.
        "timetable_summary",  # For Airflow 3.
        "start_date",
        "tags",
    ]
    casts = {"timetable": lambda dag: dag.timetable.serialize() if getattr(dag, "timetable", None) else None}
    renames = {"_dag_id": "dag_id"}


class DagRunInfo(InfoJsonEncodable):
    """Defines encoding DagRun object to JSON."""

    includes = [
        "conf",
        "dag_id",
        "data_interval_start",
        "data_interval_end",
        "external_trigger",
        "run_id",
        "run_type",
        "start_date",
    ]


class TaskInstanceInfo(InfoJsonEncodable):
    """Defines encoding TaskInstance object to JSON."""

    includes = ["duration", "try_number", "pool", "queued_dttm", "log_url"]
    casts = {
        "map_index": lambda ti: (
            ti.map_index if hasattr(ti, "map_index") and getattr(ti, "map_index") != -1 else None
        )
    }


class AssetInfo(InfoJsonEncodable):
    """Defines encoding Airflow Asset object to JSON."""

    includes = ["uri", "extra"]


class TaskInfo(InfoJsonEncodable):
    """Defines encoding BaseOperator/AbstractOperator object to JSON."""

    renames = {
        "_BaseOperator__from_mapped": "mapped",
        "_downstream_task_ids": "downstream_task_ids",
        "_upstream_task_ids": "upstream_task_ids",
        "_is_setup": "is_setup",
        "_is_teardown": "is_teardown",
    }
    includes = [
        "deferrable",
        "depends_on_past",
        "downstream_task_ids",
        "execution_timeout",
        "executor_config",
        "ignore_first_depends_on_past",
        "max_active_tis_per_dag",
        "max_active_tis_per_dagrun",
        "max_retry_delay",
        "multiple_outputs",
        "owner",
        "priority_weight",
        "queue",
        "retries",
        "retry_exponential_backoff",
        "run_as_user",
        "sla",
        "task_id",
        "trigger_dag_id",
        "external_dag_id",
        "external_task_id",
        "trigger_rule",
        "upstream_task_ids",
        "wait_for_downstream",
        "wait_for_past_depends_before_skipping",
        "weight_rule",
    ]
    casts = {
        "operator_class": lambda task: task.task_type,
        "operator_class_path": lambda task: get_fully_qualified_class_name(task),
        "task_group": lambda task: (
            TaskGroupInfo(task.task_group)
            if hasattr(task, "task_group") and getattr(task.task_group, "_group_id", None)
            else None
        ),
        "inlets": lambda task: [AssetInfo(i) for i in task.inlets if isinstance(i, Asset)],
        "outlets": lambda task: [AssetInfo(o) for o in task.outlets if isinstance(o, Asset)],
    }


class TaskInfoComplete(TaskInfo):
    """Defines encoding BaseOperator/AbstractOperator object to JSON used when user enables full task info."""

    includes = []
    excludes = [
        "_BaseOperator__instantiated",
        "_dag",
        "_hook",
        "_log",
        "_outlets",
        "_inlets",
        "_lock_for_execution",
        "handler",
        "params",
        "python_callable",
        "retry_delay",
    ]


class TaskGroupInfo(InfoJsonEncodable):
    """Defines encoding TaskGroup object to JSON."""

    renames = {
        "_group_id": "group_id",
    }
    includes = [
        "downstream_group_ids",
        "downstream_task_ids",
        "prefix_group_id",
        "tooltip",
        "upstream_group_ids",
        "upstream_task_ids",
    ]


def get_airflow_dag_run_facet(dag_run: DagRun) -> dict[str, RunFacet]:
    if not dag_run.dag:
        return {}
    return {
        "airflowDagRun": AirflowDagRunFacet(
            dag=DagInfo(dag_run.dag),
            dagRun=DagRunInfo(dag_run),
        )
    }


@conf.cache
def _get_all_packages_installed() -> dict[str, str]:
    """
    Retrieve a dictionary of all installed packages and their versions.

    This operation involves scanning the system's installed packages, which can be a heavy operation.
    It is recommended to cache the result to avoid repeated, expensive lookups.
    """
    return {dist.metadata["Name"]: dist.version for dist in metadata.distributions()}


def get_airflow_debug_facet() -> dict[str, AirflowDebugRunFacet]:
    if not conf.debug_mode():
        return {}
    log.warning("OpenLineage debug_mode is enabled. Be aware that this may log and emit extensive details.")
    return {
        "debug": AirflowDebugRunFacet(
            packages=_get_all_packages_installed(),
        )
    }


def get_airflow_run_facet(
    dag_run: DagRun,
    dag: DAG,
    task_instance: TaskInstance,
    task: BaseOperator,
    task_uuid: str,
) -> dict[str, AirflowRunFacet]:
    return {
        "airflow": AirflowRunFacet(
            dag=DagInfo(dag),
            dagRun=DagRunInfo(dag_run),
            taskInstance=TaskInstanceInfo(task_instance),
            task=TaskInfoComplete(task) if conf.include_full_task_info() else TaskInfo(task),
            taskUuid=task_uuid,
        )
    }


def get_airflow_job_facet(dag_run: DagRun) -> dict[str, AirflowJobFacet]:
    if not dag_run.dag:
        return {}
    return {
        "airflow": AirflowJobFacet(
            taskTree={},  # caused OOM errors, to be removed, see #41587
            taskGroups=_get_task_groups_details(dag_run.dag),
            tasks=_get_tasks_details(dag_run.dag),
        )
    }


def get_airflow_state_run_facet(
    dag_id: str, run_id: str, task_ids: list[str], dag_run_state: DagRunState
) -> dict[str, AirflowStateRunFacet]:
    tis = DagRun.fetch_task_instances(dag_id=dag_id, run_id=run_id, task_ids=task_ids)
    return {
        "airflowState": AirflowStateRunFacet(
            dagRunState=dag_run_state,
            tasksState={ti.task_id: ti.state for ti in tis},
        )
    }


def _get_tasks_details(dag: DAG) -> dict:
    tasks = {
        single_task.task_id: {
            "operator": get_fully_qualified_class_name(single_task),
            "task_group": single_task.task_group.group_id if single_task.task_group else None,
            "emits_ol_events": _emits_ol_events(single_task),
            "ui_color": single_task.ui_color,
            "ui_fgcolor": single_task.ui_fgcolor,
            "ui_label": single_task.label,
            "is_setup": single_task.is_setup,
            "is_teardown": single_task.is_teardown,
            "downstream_task_ids": sorted(single_task.downstream_task_ids),
        }
        for single_task in sorted(dag.tasks, key=lambda x: x.task_id)
    }

    return tasks


def _get_task_groups_details(dag: DAG) -> dict:
    return {
        tg_id: {
            "parent_group": tg.parent_group.group_id,
            "tooltip": tg.tooltip,
            "ui_color": tg.ui_color,
            "ui_fgcolor": tg.ui_fgcolor,
            "ui_label": tg.label,
        }
        for tg_id, tg in dag.task_group_dict.items()
    }


def _emits_ol_events(task: BaseOperator | MappedOperator) -> bool:
    config_selective_enabled = is_selective_lineage_enabled(task)
    config_disabled_for_operators = is_operator_disabled(task)
    # empty operators without callbacks/outlets are skipped for optimization by Airflow
    # in airflow.models.taskinstance.TaskInstance._schedule_downstream_tasks
    is_skipped_as_empty_operator = all(
        (
            task.inherits_from_empty_operator,
            not task.on_execute_callback,
            not task.on_success_callback,
            not task.outlets,
        )
    )

    emits_ol_events = all(
        (
            config_selective_enabled,
            not config_disabled_for_operators,
            not is_skipped_as_empty_operator,
        )
    )
    return emits_ol_events


def get_unknown_source_attribute_run_facet(task: BaseOperator, name: str | None = None):
    if not name:
        name = get_operator_class(task).__name__
    log.debug(
        "UnknownOperatorAttributeRunFacet is deprecated and will be removed. "
        "Use information from AirflowRunFacet instead."
    )
    return {
        "unknownSourceAttribute": attrs.asdict(
            UnknownOperatorAttributeRunFacet(
                unknownItems=[
                    UnknownOperatorInstance(
                        name=name,
                        properties=TaskInfo(task),
                    )
                ]
            )
        )
    }


class OpenLineageRedactor(SecretsMasker):
    """
    This class redacts sensitive data similar to SecretsMasker in Airflow logs.

    The difference is that our default max recursion depth is way higher - due to
    the structure of OL events we need more depth.
    Additionally, we allow data structures to specify data that needs not to be
    redacted by specifying _skip_redact list by deriving RedactMixin.
    """

    @classmethod
    def from_masker(cls, other: SecretsMasker) -> OpenLineageRedactor:
        instance = cls()
        instance.patterns = other.patterns
        instance.replacer = other.replacer
        return instance

    def _redact(self, item: Redactable, name: str | None, depth: int, max_depth: int) -> Redacted:
        if depth > max_depth:
            return item
        try:
            # It's impossible to check the type of variable in a dict without accessing it, and
            # this already causes warning - so suppress it
            with suppress(AirflowContextDeprecationWarning):
                if type(item).__name__ == "Proxy":
                    # Those are deprecated values in _DEPRECATION_REPLACEMENTS
                    # in airflow.utils.context.Context
                    return "<<non-redactable: Proxy>>"
                if name and should_hide_value_for_key(name):
                    return self._redact_all(item, depth, max_depth)
                if attrs.has(type(item)):
                    # TODO: FIXME when mypy gets compatible with new attrs
                    for dict_key, subval in attrs.asdict(
                        item,  # type: ignore[arg-type]
                        recurse=False,
                    ).items():
                        if _is_name_redactable(dict_key, item):
                            setattr(
                                item,
                                dict_key,
                                self._redact(
                                    subval,
                                    name=dict_key,
                                    depth=(depth + 1),
                                    max_depth=max_depth,
                                ),
                            )
                    return item
                elif is_json_serializable(item) and hasattr(item, "__dict__"):
                    for dict_key, subval in item.__dict__.items():
                        if type(subval).__name__ == "Proxy":
                            return "<<non-redactable: Proxy>>"
                        if _is_name_redactable(dict_key, item):
                            setattr(
                                item,
                                dict_key,
                                self._redact(
                                    subval,
                                    name=dict_key,
                                    depth=(depth + 1),
                                    max_depth=max_depth,
                                ),
                            )
                    return item
                else:
                    return super()._redact(item, name, depth, max_depth)
        except Exception as exc:
            log.warning("Unable to redact %r. Error was: %s: %s", item, type(exc).__name__, exc)
        return item


def is_json_serializable(item):
    try:
        json.dumps(item)
        return True
    except (TypeError, ValueError):
        return False


def _is_name_redactable(name, redacted):
    if not issubclass(redacted.__class__, RedactMixin):
        return not name.startswith("_")
    return name not in redacted.skip_redact


def print_warning(log):
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except Exception as e:
                log.warning(
                    "Note: exception below is being caught: it's printed for visibility. However OpenLineage events aren't being emitted. If you see that, task has completed successfully despite not getting OL events."
                )
                log.warning(e)

        return wrapper

    return decorator


def get_filtered_unknown_operator_keys(operator: BaseOperator) -> dict:
    not_required_keys = {"dag", "task_group"}
    return {attr: value for attr, value in operator.__dict__.items() if attr not in not_required_keys}


@deprecated(
    reason=(
        "`airflow.providers.openlineage.utils.utils.normalize_sql` "
        "has been deprecated and will be removed in future"
    ),
    category=AirflowProviderDeprecationWarning,
)
def normalize_sql(sql: str | Iterable[str]):
    if isinstance(sql, str):
        sql = [stmt for stmt in sql.split(";") if stmt != ""]
    sql = [obj for stmt in sql for obj in stmt.split(";") if obj != ""]
    return ";\n".join(sql)


def should_use_external_connection(hook) -> bool:
    # If we're at Airflow 2.10, the execution is process-isolated, so we can safely run those again.
    if not IS_AIRFLOW_2_10_OR_HIGHER:
        return hook.__class__.__name__ not in [
            "SnowflakeHook",
            "SnowflakeSqlApiHook",
            "RedshiftSQLHook",
        ]
    return True


def translate_airflow_asset(asset: Asset, lineage_context) -> OpenLineageDataset | None:
    """
    Convert an Asset with an AIP-60 compliant URI to an OpenLineageDataset.

    This function returns None if no URI normalizer is defined, no asset converter is found or
    some core Airflow changes are missing and ImportError is raised.
    """
    try:
        from airflow.assets import _get_normalized_scheme
    except ModuleNotFoundError:
        try:
            from airflow.datasets import _get_normalized_scheme  # type: ignore[no-redef]
        except ImportError:
            return None

    try:
        from airflow.providers_manager import ProvidersManager

        ol_converters = getattr(ProvidersManager(), "asset_to_openlineage_converters", None)
        if not ol_converters:
            ol_converters = ProvidersManager().dataset_to_openlineage_converters  # type: ignore[attr-defined]

        normalized_uri = asset.normalized_uri
    except (ImportError, AttributeError):
        return None

    if normalized_uri is None:
        return None

    if not (normalized_scheme := _get_normalized_scheme(normalized_uri)):
        return None

    if (airflow_to_ol_converter := ol_converters.get(normalized_scheme)) is None:
        return None

    return airflow_to_ol_converter(Asset(uri=normalized_uri, extra=asset.extra), lineage_context)
