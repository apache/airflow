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
import re
from contextlib import redirect_stdout, suppress
from functools import wraps
from io import StringIO
from typing import TYPE_CHECKING, Any, Iterable

import attrs
from deprecated import deprecated
from openlineage.client.utils import RedactMixin
from packaging.version import Version

from airflow import __version__ as AIRFLOW_VERSION
from airflow.exceptions import AirflowProviderDeprecationWarning  # TODO: move this maybe to Airflow's logic?
from airflow.models import DAG, BaseOperator, MappedOperator
from airflow.providers.openlineage import conf
from airflow.providers.openlineage.plugins.facets import (
    AirflowJobFacet,
    AirflowMappedTaskRunFacet,
    AirflowRunFacet,
    AirflowStateRunFacet,
    BaseFacet,
    UnknownOperatorAttributeRunFacet,
    UnknownOperatorInstance,
)
from airflow.providers.openlineage.utils.selective_enable import (
    is_dag_lineage_enabled,
    is_task_lineage_enabled,
)
from airflow.serialization.serialized_objects import SerializedBaseOperator
from airflow.utils.context import AirflowContextDeprecationWarning
from airflow.utils.log.secrets_masker import Redactable, Redacted, SecretsMasker, should_hide_value_for_key
from airflow.utils.module_loading import import_string

if TYPE_CHECKING:
    from airflow.models import DagRun, TaskInstance


log = logging.getLogger(__name__)
_NOMINAL_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
_IS_AIRFLOW_2_10_OR_HIGHER = Version(Version(AIRFLOW_VERSION).base_version) >= Version("2.10.0")


def try_import_from_string(string: str) -> Any:
    with suppress(ImportError):
        return import_string(string)


def get_operator_class(task: BaseOperator) -> type:
    if task.__class__.__name__ in ("DecoratedMappedOperator", "MappedOperator"):
        return task.operator_class
    return task.__class__


def get_job_name(task: TaskInstance) -> str:
    return f"{task.dag_id}.{task.task_id}"


def get_custom_facets(task_instance: TaskInstance | None = None) -> dict[str, Any]:
    custom_facets = {}
    # check for -1 comes from SmartSensor compatibility with dynamic task mapping
    # this comes from Airflow code
    if hasattr(task_instance, "map_index") and getattr(task_instance, "map_index") != -1:
        custom_facets["airflow_mappedTask"] = AirflowMappedTaskRunFacet.from_task_instance(task_instance)
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
        if isinstance(value, (set, tuple)):
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
            for field, val in self.obj.__dict__.items():
                if field not in self._fields and field not in self.excludes and field not in self.renames:
                    setattr(self, field, val)
                    self._fields.append(field)


class DagInfo(InfoJsonEncodable):
    """Defines encoding DAG object to JSON."""

    includes = ["dag_id", "description", "owner", "schedule_interval", "start_date", "tags"]
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

    includes = ["duration", "try_number", "pool", "queued_dttm"]
    casts = {
        "map_index": lambda ti: (
            ti.map_index if hasattr(ti, "map_index") and getattr(ti, "map_index") != -1 else None
        )
    }


class DatasetInfo(InfoJsonEncodable):
    """Defines encoding Airflow Dataset object to JSON."""

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
        "task_group": lambda task: (
            TaskGroupInfo(task.task_group)
            if hasattr(task, "task_group") and getattr(task.task_group, "_group_id", None)
            else None
        ),
        "inlets": lambda task: [DatasetInfo(inlet) for inlet in task.inlets],
        "outlets": lambda task: [DatasetInfo(outlet) for outlet in task.outlets],
    }


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


def get_airflow_run_facet(
    dag_run: DagRun,
    dag: DAG,
    task_instance: TaskInstance,
    task: BaseOperator,
    task_uuid: str,
) -> dict[str, BaseFacet]:
    return {
        "airflow": AirflowRunFacet(
            dag=DagInfo(dag),
            dagRun=DagRunInfo(dag_run),
            taskInstance=TaskInstanceInfo(task_instance),
            task=TaskInfo(task),
            taskUuid=task_uuid,
        )
    }


def get_airflow_job_facet(dag_run: DagRun) -> dict[str, BaseFacet]:
    if not dag_run.dag:
        return {}
    return {
        "airflow": AirflowJobFacet(
            taskTree=_get_parsed_dag_tree(dag_run.dag),
            taskGroups=_get_task_groups_details(dag_run.dag),
            tasks=_get_tasks_details(dag_run.dag),
        )
    }


def get_airflow_state_run_facet(dag_run: DagRun) -> dict[str, BaseFacet]:
    return {
        "airflowState": AirflowStateRunFacet(
            dagRunState=dag_run.get_state(),
            tasksState={ti.task_id: ti.state for ti in dag_run.get_task_instances()},
        )
    }


def _safe_get_dag_tree_view(dag: DAG) -> list[str]:
    # get_tree_view() has been added in Airflow 2.8.2
    if hasattr(dag, "get_tree_view"):
        return dag.get_tree_view().splitlines()

    with redirect_stdout(StringIO()) as stdout:
        dag.tree_view()
        return stdout.getvalue().splitlines()


def _get_parsed_dag_tree(dag: DAG) -> dict:
    """
    Get DAG's tasks hierarchy representation.

    While the task dependencies are defined as following:
    task >> [task_2, task_4] >> task_7
    task_3 >> task_5
    task_6  # has no dependencies, it's a root and a leaf

    The result of this function will look like:
    {
        "task": {
            "task_2": {
                "task_7": {}
            },
            "task_4": {
                "task_7": {}
            }
        },
        "task_3": {
            "task_5": {}
        },
        "task_6": {}
    }
    """
    lines = _safe_get_dag_tree_view(dag)
    task_dict: dict[str, dict] = {}
    parent_map: dict[int, tuple[str, dict]] = {}

    for line in lines:
        stripped_line = line.strip()
        if not stripped_line:
            continue

        # Determine the level by counting the leading spaces, assuming 4 spaces per level
        # as defined in airflow.models.dag.DAG._generate_tree_view()
        level = (len(line) - len(stripped_line)) // 4
        # airflow.models.baseoperator.BaseOperator.__repr__ is used in DAG tree
        # <Task({op_class}): {task_id}>
        match = re.match(r"^<Task\((.+)\): (.*?)>$", stripped_line)
        if not match:
            return {}
        current_task_id = match[2]

        if level == 0:  # It's a root task
            task_dict[current_task_id] = {}
            parent_map[level] = (current_task_id, task_dict[current_task_id])
        else:
            # Find the immediate parent task
            parent_task, parent_dict = parent_map[(level - 1)]
            # Create new dict for the current task
            parent_dict[current_task_id] = {}
            # Update this task in the parent map
            parent_map[level] = (current_task_id, parent_dict[current_task_id])

    return task_dict


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
        }
        for single_task in dag.tasks
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
        (config_selective_enabled, not config_disabled_for_operators, not is_skipped_as_empty_operator)
    )
    return emits_ol_events


def get_unknown_source_attribute_run_facet(task: BaseOperator, name: str | None = None):
    if not name:
        name = get_operator_class(task).__name__
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
                                self._redact(subval, name=dict_key, depth=(depth + 1), max_depth=max_depth),
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
                                self._redact(subval, name=dict_key, depth=(depth + 1), max_depth=max_depth),
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
    if not _IS_AIRFLOW_2_10_OR_HIGHER:
        return hook.__class__.__name__ not in ["SnowflakeHook", "SnowflakeSqlApiHook", "RedshiftSQLHook"]
    return True
