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
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import attrs
from attrs import asdict

from airflow.providers.openlineage.plugins.facets import (
    AirflowMappedTaskRunFacet,
    AirflowRunFacet,
)
from airflow.utils.log.secrets_masker import Redactable, Redacted, SecretsMasker, should_hide_value_for_key

# TODO: move this maybe to Airflow's logic?
from openlineage.client.utils import RedactMixin

if TYPE_CHECKING:
    from airflow.models import DAG, BaseOperator, Connection, DagRun, TaskInstance


log = logging.getLogger(__name__)
_NOMINAL_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


def openlineage_job_name(dag_id: str, task_id: str) -> str:
    return f"{dag_id}.{task_id}"


def get_operator_class(task: BaseOperator) -> type:
    if task.__class__.__name__ in ("DecoratedMappedOperator", "MappedOperator"):
        return task.operator_class
    return task.__class__


def to_json_encodable(task: BaseOperator) -> dict[str, object]:
    def _task_encoder(obj):
        from airflow.models import DAG

        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        elif isinstance(obj, DAG):
            return {
                "dag_id": obj.dag_id,
                "tags": obj.tags,
                "schedule_interval": obj.schedule_interval,
                "timetable": obj.timetable.serialize(),
            }
        else:
            return str(obj)

    return json.loads(json.dumps(task.__dict__, default=_task_encoder))


def url_to_https(url) -> str | None:
    # Ensure URL exists
    if not url:
        return None

    base_url = None
    if url.startswith("git@"):
        part = url.split("git@")[1:2]
        if part:
            base_url = f'https://{part[0].replace(":", "/", 1)}'
    elif url.startswith("https://"):
        base_url = url

    if not base_url:
        raise ValueError(f"Unable to extract location from: {url}")

    if base_url.endswith(".git"):
        base_url = base_url[:-4]
    return base_url


def redacted_connection_uri(conn: Connection, filtered_params=None, filtered_prefixes=None):
    """
    Return the connection URI for the given Connection.
    This method additionally filters URI by removing query parameters that are known to carry sensitive data
    like username, password, access key.
    """
    if filtered_prefixes is None:
        filtered_prefixes = []
    if filtered_params is None:
        filtered_params = []

    def filter_key_params(k: str):
        return k not in filtered_params and any(substr in k for substr in filtered_prefixes)

    conn_uri = conn.get_uri()
    parsed = urlparse(conn_uri)

    # Remove username and password
    netloc = f"{parsed.hostname}" + (f":{parsed.port}" if parsed.port else "")
    parsed = parsed._replace(netloc=netloc)
    if parsed.query:
        query_dict = dict(parse_qsl(parsed.query))
        if conn.EXTRA_KEY in query_dict:
            query_dict = json.loads(query_dict[conn.EXTRA_KEY])
        filtered_qs = {k: v for k, v in query_dict.items() if not filter_key_params(k)}
        parsed = parsed._replace(query=urlencode(filtered_qs))
    return urlunparse(parsed)


def get_connection(conn_id) -> Connection | None:
    from airflow.hooks.base import BaseHook

    with suppress(Exception):
        return BaseHook.get_connection(conn_id=conn_id)
    return None


def get_job_name(task):
    return f"{task.dag_id}.{task.task_id}"


def get_custom_facets(task_instance: TaskInstance | None = None) -> dict[str, Any]:
    custom_facets = {}
    # check for -1 comes from SmartSensor compatibility with dynamic task mapping
    # this comes from Airflow code
    if hasattr(task_instance, "map_index") and getattr(task_instance, "map_index") != -1:
        custom_facets["airflow_mappedTask"] = AirflowMappedTaskRunFacet.from_task_instance(task_instance)
    return custom_facets


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
            raise Exception("Don't use both includes and excludes.")
        if self.includes:
            for field in self.includes:
                if field in self._fields or not hasattr(self.obj, field):
                    continue
                setattr(self, field, getattr(self.obj, field))
                self._fields.append(field)
        else:
            for field, val in self.obj.__dict__.items():
                if field in self._fields or field in self.excludes or field in self.renames:
                    continue
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
        "_BaseOperator__init_kwargs": "args",
        "_BaseOperator__from_mapped": "mapped",
        "_downstream_task_ids": "downstream_task_ids",
        "_upstream_task_ids": "upstream_task_ids",
    }
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
        "airflow": json.loads(
            json.dumps(
                asdict(
                    AirflowRunFacet(
                        dag=DagInfo(dag),
                        dagRun=DagRunInfo(dag_run),
                        taskInstance=TaskInstanceInfo(task_instance),
                        task=TaskInfo(task),
                        taskUuid=task_uuid,
                    )
                ),
                default=str,
            )
        )
    }


class OpenLineageRedactor(SecretsMasker):
    """This class redacts sensitive data similar to SecretsMasker in Airflow logs.
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
            if name and should_hide_value_for_key(name):
                return self._redact_all(item, depth, max_depth)
            if attrs.has(type(item)):
                # TODO: fixme when mypy gets compatible with new attrs
                for dict_key, subval in attrs.asdict(item, recurse=False).items():  # type: ignore[arg-type]
                    if _is_name_redactable(dict_key, item):
                        setattr(
                            item,
                            dict_key,
                            self._redact(subval, name=dict_key, depth=(depth + 1), max_depth=max_depth),
                        )
                return item
            elif is_json_serializable(item) and hasattr(item, "__dict__"):
                for dict_key, subval in item.__dict__.items():
                    if _is_name_redactable(dict_key, item):
                        setattr(
                            item,
                            dict_key,
                            self._redact(subval, name=dict_key, depth=(depth + 1), max_depth=max_depth),
                        )
                return item
            else:
                return super()._redact(item, name, depth, max_depth)
        except Exception as e:
            log.warning(
                "Unable to redact %s" "Error was: %s: %s",
                repr(item),
                type(e).__name__,
                str(e),
            )
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


def print_exception(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as e:
            log.exception(e)

    return wrapper
