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
from typing import TYPE_CHECKING, Any, Iterable

import attrs
from openlineage.client.utils import RedactMixin  # TODO: move this maybe to Airflow's logic?

from airflow.models import DAG, BaseOperator, MappedOperator
from airflow.providers.openlineage import conf
from airflow.providers.openlineage.plugins.facets import (
    AirflowMappedTaskRunFacet,
    AirflowRunFacet,
    UnknownOperatorAttributeRunFacet,
    UnknownOperatorInstance,
)
from airflow.providers.openlineage.utils.selective_enable import (
    is_dag_lineage_enabled,
    is_task_lineage_enabled,
)
from airflow.utils.context import AirflowContextDeprecationWarning
from airflow.utils.log.secrets_masker import Redactable, Redacted, SecretsMasker, should_hide_value_for_key

if TYPE_CHECKING:
    from airflow.models import DagRun, TaskInstance


log = logging.getLogger(__name__)
_NOMINAL_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


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
    return operator.__class__.__module__ + "." + operator.__class__.__name__


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
            for field, val in self.obj.__dict__.items():
                if field not in self._fields and field not in self.excludes and field not in self.renames:
                    setattr(self, field, val)
                    self._fields.append(field)


class DagInfo(InfoJsonEncodable):
    """Defines encoding DAG object to JSON."""

    includes = ["dag_id", "schedule_interval", "tags", "start_date"]
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

    includes = ["duration", "try_number", "pool"]
    casts = {
        "map_index": lambda ti: ti.map_index
        if hasattr(ti, "map_index") and getattr(ti, "map_index") != -1
        else None
    }


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
        "task_id",
        "trigger_rule",
        "upstream_task_ids",
        "wait_for_downstream",
        "wait_for_past_depends_before_skipping",
        "weight_rule",
    ]
    casts = {
        "operator_class": lambda task: task.task_type,
        "task_group": lambda task: TaskGroupInfo(task.task_group)
        if hasattr(task, "task_group") and getattr(task.task_group, "_group_id", None)
        else None,
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
):
    return {
        "airflow": attrs.asdict(
            AirflowRunFacet(
                dag=DagInfo(dag),
                dagRun=DagRunInfo(dag_run),
                taskInstance=TaskInstanceInfo(task_instance),
                task=TaskInfo(task),
                taskUuid=task_uuid,
            )
        )
    }


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
                log.warning(e)

        return wrapper

    return decorator


def get_filtered_unknown_operator_keys(operator: BaseOperator) -> dict:
    not_required_keys = {"dag", "task_group"}
    return {attr: value for attr, value in operator.__dict__.items() if attr not in not_required_keys}


def normalize_sql(sql: str | Iterable[str]):
    if isinstance(sql, str):
        sql = [stmt for stmt in sql.split(";") if stmt != ""]
    sql = [obj for stmt in sql for obj in stmt.split(";") if obj != ""]
    return ";\n".join(sql)
