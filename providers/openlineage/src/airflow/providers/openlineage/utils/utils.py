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
from collections.abc import Callable
from contextlib import suppress
from functools import wraps
from importlib import metadata
from typing import TYPE_CHECKING, Any

import attrs
from openlineage.client.facet_v2 import parent_run
from openlineage.client.utils import RedactMixin

from airflow import __version__ as AIRFLOW_VERSION
from airflow.exceptions import AirflowOptionalProviderFeatureException

# TODO: move this maybe to Airflow's logic?
from airflow.models import DagRun, TaskInstance, TaskReschedule
from airflow.providers.common.compat.assets import Asset
from airflow.providers.common.compat.module_loading import import_string
from airflow.providers.common.compat.sdk import DAG, BaseOperator, BaseSensorOperator, MappedOperator
from airflow.providers.openlineage import (
    __version__ as OPENLINEAGE_PROVIDER_VERSION,
    conf,
)
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
from airflow.providers.openlineage.version_compat import AIRFLOW_V_3_0_PLUS, get_base_airflow_version_tuple
from airflow.serialization.serialized_objects import SerializedBaseOperator, SerializedDAG

try:
    from airflow.serialization.definitions.mappedoperator import SerializedMappedOperator
except ImportError:
    from airflow.models.mappedoperator import (  # type: ignore[attr-defined,no-redef]
        MappedOperator as SerializedMappedOperator,
    )

if not AIRFLOW_V_3_0_PLUS:
    from airflow.utils.session import NEW_SESSION, provide_session

if TYPE_CHECKING:
    from typing import TypeAlias

    from openlineage.client.event_v2 import Dataset as OpenLineageDataset
    from openlineage.client.facet_v2 import RunFacet, processing_engine_run

    from airflow.sdk.execution_time.secrets_masker import (
        Redactable,
        Redacted,
        SecretsMasker,
    )
    from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance
    from airflow.utils.state import DagRunState, TaskInstanceState

    AnyOperator: TypeAlias = BaseOperator | MappedOperator | SerializedBaseOperator | SerializedMappedOperator
else:
    try:
        from airflow.sdk._shared.secrets_masker import (
            Redactable,
            Redacted,
            SecretsMasker,
            should_hide_value_for_key,
        )
    except ImportError:
        try:
            from airflow.sdk.execution_time.secrets_masker import (
                Redactable,
                Redacted,
                SecretsMasker,
                should_hide_value_for_key,
            )
        except ImportError:
            from airflow.utils.log.secrets_masker import (
                Redactable,
                Redacted,
                SecretsMasker,
                should_hide_value_for_key,
            )

log = logging.getLogger(__name__)
_NOMINAL_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
_MAX_DOC_BYTES = 64 * 1024  # 64 kilobytes


def try_import_from_string(string: str) -> Any:
    with suppress(ImportError):
        return import_string(string)


def get_operator_class(task: BaseOperator | MappedOperator) -> type:
    if task.__class__.__name__ in ("DecoratedMappedOperator", "MappedOperator"):
        return task.operator_class
    return task.__class__


def get_operator_provider_version(operator: AnyOperator) -> str | None:
    """Get the provider package version for the given operator."""
    try:
        class_path = get_fully_qualified_class_name(operator)

        if not class_path.startswith("airflow.providers."):
            return None

        from airflow.providers_manager import ProvidersManager

        providers_manager = ProvidersManager()

        for package_name, provider_info in providers_manager.providers.items():
            if package_name.startswith("apache-airflow-providers-"):
                provider_module_path = package_name.replace(
                    "apache-airflow-providers-", "airflow.providers."
                ).replace("-", ".")
                if class_path.startswith(provider_module_path + "."):
                    return provider_info.version

        return None

    except Exception:
        return None


def get_job_name(task: TaskInstance | RuntimeTaskInstance) -> str:
    return f"{task.dag_id}.{task.task_id}"


def _get_parent_run_facet(
    parent_run_id: str,
    parent_job_name: str,
    parent_job_namespace: str = conf.namespace(),
    root_parent_run_id: str | None = None,
    root_parent_job_name: str | None = None,
    root_parent_job_namespace: str | None = None,
) -> dict[str, Any]:
    """Create the parent run facet from identifiers."""
    root_parent_run_id = root_parent_run_id or parent_run_id
    root_parent_job_name = root_parent_job_name or parent_job_name
    root_parent_job_namespace = root_parent_job_namespace or parent_job_namespace
    return {
        "parent": parent_run.ParentRunFacet(
            run=parent_run.Run(runId=parent_run_id),
            job=parent_run.Job(namespace=parent_job_namespace, name=parent_job_name),
            root=parent_run.Root(
                run=parent_run.RootRun(runId=root_parent_run_id),
                job=parent_run.RootJob(namespace=root_parent_job_namespace, name=root_parent_job_name),
            ),
        )
    }


def get_task_parent_run_facet(
    parent_run_id: str,
    parent_job_name: str,
    parent_job_namespace: str = conf.namespace(),
    root_parent_run_id: str | None = None,
    root_parent_job_name: str | None = None,
    root_parent_job_namespace: str | None = None,
    dr_conf: dict | None = None,
) -> dict[str, Any]:
    """Retrieve the parent run facet."""
    all_root_info = (root_parent_run_id, root_parent_job_namespace, root_parent_job_name)
    # If not all root identifiers are provided explicitly, try to get them from dagrun conf.
    if not all(all_root_info):
        # If some but not all root identifiers are provided warn and do not use any of them, we need all.
        if any(all_root_info):
            log.warning(
                "Incomplete root OpenLineage information provided. "
                "No root information will be used. Found values: "
                "root_parent_run_id='%s', root_parent_job_namespace='%s', root_parent_job_name='%s'.",
                root_parent_run_id,
                root_parent_job_namespace,
                root_parent_job_name,
            )
            root_parent_run_id, root_parent_job_namespace, root_parent_job_name = None, None, None
        elif dr_conf:
            # Check for root identifiers in dagrun conf
            if root_info := get_root_information_from_dagrun_conf(dr_conf):
                root_parent_run_id = root_info["root_parent_run_id"]
                root_parent_job_namespace = root_info["root_parent_job_namespace"]
                root_parent_job_name = root_info["root_parent_job_name"]
            # If not present, check for parent identifiers in dagrun conf and use them as root
            elif parent_info := get_parent_information_from_dagrun_conf(dr_conf):
                root_parent_run_id = parent_info["parent_run_id"]
                root_parent_job_namespace = parent_info["parent_job_namespace"]
                root_parent_job_name = parent_info["parent_job_name"]

    return _get_parent_run_facet(
        parent_run_id=parent_run_id,
        parent_job_namespace=parent_job_namespace,
        parent_job_name=parent_job_name,
        root_parent_run_id=root_parent_run_id,
        root_parent_job_namespace=root_parent_job_namespace,
        root_parent_job_name=root_parent_job_name,
    )


def _get_openlineage_data_from_dagrun_conf(dr_conf: dict | None) -> dict:
    """Return the 'openlineage' section from a DAG run config if valid, otherwise an empty dict."""
    if not dr_conf or not isinstance(dr_conf, dict):
        return {}
    ol_data = dr_conf.get("openlineage")
    return ol_data if isinstance(ol_data, dict) else {}


def get_root_information_from_dagrun_conf(dr_conf: dict | None) -> dict[str, str]:
    """Extract root parent run and job information from a DAG run config."""
    ol_data = _get_openlineage_data_from_dagrun_conf(dr_conf)
    if not ol_data:
        log.debug("No 'openlineage' data found in DAG run config.")
        return {}

    root_parent_run_id = ol_data.get("rootParentRunId", "")
    root_parent_job_namespace = ol_data.get("rootParentJobNamespace", "")
    root_parent_job_name = ol_data.get("rootParentJobName", "")

    all_root_info = (root_parent_run_id, root_parent_job_namespace, root_parent_job_name)
    if not all(all_root_info):
        if any(all_root_info):
            log.warning(
                "Incomplete root OpenLineage information in DAG run config. "
                "No root information will be used. Found values: "
                "rootParentRunId='%s', rootParentJobNamespace='%s', rootParentJobName='%s'.",
                root_parent_run_id,
                root_parent_job_namespace,
                root_parent_job_name,
            )
        else:
            log.debug("No 'openlineage' root information found in DAG run config.")
        return {}

    try:  # Validate that runId is correct UUID
        parent_run.RootRun(runId=root_parent_run_id)
    except ValueError:
        log.warning(
            "Invalid OpenLineage rootParentRunId '%s' in DAG run config - expected a valid UUID.",
            root_parent_run_id,
        )
        return {}

    log.debug(
        "Extracted valid root OpenLineage identifiers from DAG run config: "
        "rootParentRunId='%s', rootParentJobNamespace='%s', rootParentJobName='%s'.",
        root_parent_run_id,
        root_parent_job_namespace,
        root_parent_job_name,
    )
    return {
        "root_parent_run_id": root_parent_run_id,
        "root_parent_job_namespace": root_parent_job_namespace,
        "root_parent_job_name": root_parent_job_name,
    }


def get_parent_information_from_dagrun_conf(dr_conf: dict | None) -> dict[str, str]:
    """Extract parent run and job information from a DAG run config."""
    ol_data = _get_openlineage_data_from_dagrun_conf(dr_conf)
    if not ol_data:
        log.debug("No 'openlineage' data found in DAG run config.")
        return {}

    parent_run_id = ol_data.get("parentRunId", "")
    parent_job_namespace = ol_data.get("parentJobNamespace", "")
    parent_job_name = ol_data.get("parentJobName", "")

    all_parent_info = (parent_run_id, parent_job_namespace, parent_job_name)
    if not all(all_parent_info):
        if any(all_parent_info):
            log.warning(
                "Incomplete parent OpenLineage information in DAG run config. "
                "ParentRunFacet will NOT be created. Found values: "
                "parentRunId='%s', parentJobNamespace='%s', parentJobName='%s'.",
                parent_run_id,
                parent_job_namespace,
                parent_job_name,
            )
        else:
            log.debug("No 'openlineage' parent information found in DAG run config.")
        return {}

    try:  # Validate that runId is correct UUID
        parent_run.RootRun(runId=parent_run_id)
    except ValueError:
        log.warning(
            "Invalid OpenLineage parentRunId '%s' in DAG run config - expected a valid UUID.",
            parent_run_id,
        )
        return {}

    log.debug(
        "Extracted valid parent OpenLineage identifiers from DAG run config: "
        "parentRunId='%s', parentJobNamespace='%s', parentJobName='%s'.",
        parent_run_id,
        parent_job_namespace,
        parent_job_name,
    )

    return {
        "parent_run_id": parent_run_id,
        "parent_job_namespace": parent_job_namespace,
        "parent_job_name": parent_job_name,
    }


def get_dag_parent_run_facet(dr_conf: dict | None) -> dict[str, parent_run.ParentRunFacet]:
    """
    Build the OpenLineage parent run facet from a DAG run configuration.

    This function extracts parent run identifiers - run ID, job namespace, and job name -
    from the DAG run configuration to construct an OpenLineage `ParentRunFacet`. It requires
    a complete set of parent identifiers to proceed; if some but not all are present, or if
    the run ID is invalid, the function returns an empty dictionary.

    When valid parent identifiers are found, it also attempts to retrieve corresponding
    root identifiers using `get_root_information_from_dagrun_conf`, which may fall back
    to the parent identifiers if no explicit root data is available. The resulting facet
    links both the immediate parent and root lineage information for the run.

    Args:
        dr_conf: The DAG run configuration dictionary.

    Returns:
        A dictionary containing a single entry mapping the facet name to the constructed
        `ParentRunFacet`. Returns an empty dictionary if the configuration does not contain
        a complete or valid set of parent identifiers.
    """
    ol_data = _get_openlineage_data_from_dagrun_conf(dr_conf)
    if not ol_data:
        log.debug("No 'openlineage' data found in DAG run config.")
        return {}

    parent_info = get_parent_information_from_dagrun_conf(dr_conf)
    if not parent_info:  # Validation and logging is done in the above function
        return {}

    root_parent_run_id, root_parent_job_namespace, root_parent_job_name = None, None, None
    if root_info := get_root_information_from_dagrun_conf(dr_conf):
        root_parent_run_id = root_info["root_parent_run_id"]
        root_parent_job_namespace = root_info["root_parent_job_namespace"]
        root_parent_job_name = root_info["root_parent_job_name"]
    else:
        log.debug(
            "Missing OpenLineage root identifiers in DAG run config, "
            "parent identifiers will be used as root instead."
        )

    # Function already uses parent as root if root is missing, no need to explicitly pass it
    return _get_parent_run_facet(
        parent_run_id=parent_info["parent_run_id"],
        parent_job_namespace=parent_info["parent_job_namespace"],
        parent_job_name=parent_info["parent_job_name"],
        root_parent_run_id=root_parent_run_id,
        root_parent_job_namespace=root_parent_job_namespace,
        root_parent_job_name=root_parent_job_name,
    )


def _truncate_string_to_byte_size(s: str, max_size: int = _MAX_DOC_BYTES) -> str:
    """
    Truncate a string to a maximum UTF-8 byte size, ensuring valid encoding.

    This is used to safely limit the size of string content (e.g., for OpenLineage events)
    without breaking multibyte characters. If truncation occurs, the result is a valid
    UTF-8 string with any partial characters at the end removed.

    Args:
        s (str): The input string to truncate.
        max_size (int): Maximum allowed size in bytes after UTF-8 encoding.

    Returns:
        str: A UTF-8-safe truncated string within the specified byte limit.
    """
    encoded = s.encode("utf-8")
    if len(encoded) <= max_size:
        return s
    log.debug(
        "Truncating long string content for OpenLineage event. "
        "Original size: %d bytes, truncated to: %d bytes (UTF-8 safe).",
        len(encoded),
        max_size,
    )
    truncated = encoded[:max_size]
    # Make sure we don't cut a multibyte character in half
    return truncated.decode("utf-8", errors="ignore")


def get_task_documentation(operator: AnyOperator | None) -> tuple[str | None, str | None]:
    """Get task documentation and mime type, truncated to _MAX_DOC_BYTES bytes length, if present."""
    if not operator:
        return None, None

    doc, mime_type = None, None
    if operator.doc:
        doc = operator.doc
        mime_type = "text/plain"
    elif operator.doc_md:
        doc = operator.doc_md
        mime_type = "text/markdown"
    elif operator.doc_json:
        doc = operator.doc_json
        mime_type = "application/json"
    elif operator.doc_yaml:
        doc = operator.doc_yaml
        mime_type = "application/x-yaml"
    elif operator.doc_rst:
        doc = operator.doc_rst
        mime_type = "text/x-rst"

    if doc:
        return _truncate_string_to_byte_size(doc), mime_type
    return None, None


def get_dag_documentation(dag: DAG | SerializedDAG | None) -> tuple[str | None, str | None]:
    """Get dag documentation and mime type, truncated to _MAX_DOC_BYTES bytes length, if present."""
    if not dag:
        return None, None

    doc, mime_type = None, None
    if dag.doc_md:
        doc = dag.doc_md
        mime_type = "text/markdown"
    elif dag.description:
        doc = dag.description
        mime_type = "text/plain"

    if doc:
        return _truncate_string_to_byte_size(doc), mime_type
    return None, None


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


def get_fully_qualified_class_name(operator: AnyOperator) -> str:
    if isinstance(operator, (SerializedMappedOperator, SerializedBaseOperator)):
        # as in airflow.api_connexion.schemas.common_schema.ClassReferenceSchema
        return operator._task_module + "." + operator.task_type
    op_class = get_operator_class(operator)
    return op_class.__module__ + "." + op_class.__name__


def is_operator_disabled(operator: AnyOperator) -> bool:
    return get_fully_qualified_class_name(operator) in conf.disabled_operators()


def is_selective_lineage_enabled(obj: DAG | SerializedDAG | AnyOperator) -> bool:
    """If selective enable is active check if DAG or Task is enabled to emit events."""
    if not conf.selective_enable():
        return True
    if isinstance(obj, (DAG, SerializedDAG)):
        return is_dag_lineage_enabled(obj)
    if isinstance(obj, (BaseOperator, MappedOperator)):
        return is_task_lineage_enabled(obj)
    raise TypeError("is_selective_lineage_enabled can only be used on DAG or Operator objects")


if not AIRFLOW_V_3_0_PLUS:

    @provide_session
    def is_ti_rescheduled_already(ti: TaskInstance, session=NEW_SESSION):
        try:
            from sqlalchemy import exists, select
        except ImportError:
            raise AirflowOptionalProviderFeatureException(
                "sqlalchemy is required for checking task instance reschedule status. "
                "Install it with: pip install 'apache-airflow-providers-openlineage[sqlalchemy]'"
            )

        if not isinstance(ti.task, BaseSensorOperator):
            return False

        if not ti.task.reschedule:
            return False
        return (
            session.scalar(
                select(
                    exists().where(
                        TaskReschedule.dag_id == ti.dag_id,
                        TaskReschedule.task_id == ti.task_id,
                        TaskReschedule.run_id == ti.run_id,
                        TaskReschedule.map_index == ti.map_index,
                        TaskReschedule.try_number == ti.try_number,
                    )
                )
            )
            is True
        )


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
        del self.obj

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
        "owner_links",
        "schedule_interval",  # For Airflow 2 only -> AF3 has timetable_summary
        "start_date",
        "tags",
    ]
    casts = {
        "timetable": lambda dag: DagInfo.serialize_timetable(dag),
        "timetable_summary": lambda dag: DagInfo.timetable_summary(dag),
    }
    renames = {"_dag_id": "dag_id"}

    @classmethod
    def timetable_summary(cls, dag: DAG | SerializedDAG) -> str | None:
        """Extract summary from timetable if missing a ``timetable_summary`` property."""
        if summary := getattr(dag, "timetable_summary", None):
            return summary
        if (timetable := getattr(dag, "timetable", None)) is None:
            return None
        if summary := getattr(timetable, "summary", None):
            return summary
        with suppress(ImportError):
            from airflow.serialization.encoders import coerce_to_core_timetable

            return coerce_to_core_timetable(timetable).summary
        return None

    @classmethod
    def serialize_timetable(cls, dag: DAG | SerializedDAG) -> dict[str, Any]:
        # This is enough for Airflow 2.10+ and has all the information needed
        try:
            serialized = dag.timetable.serialize() or {}  # type: ignore[union-attr]
        except AttributeError:
            from airflow.serialization.encoders import encode_timetable

            serialized = encode_timetable(dag.timetable)["__var"]

        # In Airflow 2.9 when using Dataset scheduling we do not receive datasets in serialized timetable
        # Also for DatasetOrTimeSchedule, we only receive timetable without dataset_condition
        if hasattr(dag, "dataset_triggers") and "dataset_condition" not in serialized:
            try:
                # Make sure we are in Airflow version where these are importable
                from airflow.datasets import BaseDatasetEventInput, DatasetAll, DatasetAny
            except ImportError:
                log.warning("OpenLineage could not serialize full dag's timetable for dag `%s`.", dag.dag_id)
                return serialized

            def _serialize_ds(ds: BaseDatasetEventInput) -> dict[str, Any]:
                if isinstance(ds, (DatasetAny, DatasetAll)):
                    return {
                        "__type": "dataset_all" if isinstance(ds, DatasetAll) else "dataset_any",
                        "objects": [_serialize_ds(child) for child in ds.objects],
                    }
                return {"__type": "dataset", "uri": ds.uri, "extra": ds.extra}

            if isinstance(dag.dataset_triggers, BaseDatasetEventInput):
                serialized["dataset_condition"] = _serialize_ds(dag.dataset_triggers)
            elif isinstance(dag.dataset_triggers, list) and len(dag.dataset_triggers):
                serialized["dataset_condition"] = {
                    "__type": "dataset_all",
                    "objects": [_serialize_ds(trigger) for trigger in dag.dataset_triggers],
                }
        return serialized


class DagRunInfo(InfoJsonEncodable):
    """Defines encoding DagRun object to JSON."""

    includes = [
        "conf",
        "dag_id",
        "data_interval_start",
        "data_interval_end",
        "external_trigger",  # Removed in Airflow 3, use run_type instead
        "execution_date",  # Airflow 2
        "logical_date",  # Airflow 3
        "run_after",  # Airflow 3
        "run_id",
        "run_type",
        "start_date",
        "end_date",
    ]

    casts = {
        "duration": lambda dagrun: DagRunInfo.duration(dagrun),
        "dag_bundle_name": lambda dagrun: DagRunInfo.dag_version_info(dagrun, "bundle_name"),
        "dag_bundle_version": lambda dagrun: DagRunInfo.dag_version_info(dagrun, "bundle_version"),
        "dag_version_id": lambda dagrun: DagRunInfo.dag_version_info(dagrun, "version_id"),
        "dag_version_number": lambda dagrun: DagRunInfo.dag_version_info(dagrun, "version_number"),
    }

    @classmethod
    def duration(cls, dagrun: DagRun) -> float | None:
        if not getattr(dagrun, "end_date", None) or not isinstance(dagrun.end_date, datetime.datetime):
            return None
        if not getattr(dagrun, "start_date", None) or not isinstance(dagrun.start_date, datetime.datetime):
            return None
        return (dagrun.end_date - dagrun.start_date).total_seconds()

    @classmethod
    def dag_version_info(cls, dagrun: DagRun, key: str) -> str | int | None:
        # AF2 DagRun and AF3 DagRun SDK model (on worker) do not have this information
        if not getattr(dagrun, "dag_versions", []):
            return None
        current_version = dagrun.dag_versions[-1]
        if key == "bundle_name":
            return current_version.bundle_name
        if key == "bundle_version":
            return current_version.bundle_version
        if key == "version_id":
            return str(current_version.id)
        if key == "version_number":
            return current_version.version_number
        raise ValueError(f"Unsupported key: {key}`")


class TaskInstanceInfo(InfoJsonEncodable):
    """Defines encoding TaskInstance object to JSON."""

    includes = ["duration", "try_number", "pool", "queued_dttm", "log_url"]
    casts = {
        "log_url": lambda ti: getattr(ti, "log_url", None),
        "map_index": lambda ti: ti.map_index if getattr(ti, "map_index", -1) != -1 else None,
        "dag_bundle_version": lambda ti: (
            ti.bundle_instance.version if hasattr(ti, "bundle_instance") else None
        ),
        "dag_bundle_name": lambda ti: ti.bundle_instance.name if hasattr(ti, "bundle_instance") else None,
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
        "trigger_rule",
        "upstream_task_ids",
        "wait_for_downstream",
        "wait_for_past_depends_before_skipping",
        # Operator-specific useful attributes
        "trigger_dag_id",  # TriggerDagRunOperator
        "trigger_run_id",  # TriggerDagRunOperator
        "external_dag_id",  # ExternalTaskSensor and ExternalTaskMarker (if run, as it's EmptyOperator)
        "external_task_id",  # ExternalTaskSensor and ExternalTaskMarker (if run, as it's EmptyOperator)
        "external_task_ids",  # ExternalTaskSensor
        "external_task_group_id",  # ExternalTaskSensor
        "external_dates_filter",  # ExternalTaskSensor
        "logical_date",  # AF 3 ExternalTaskMarker (if run, as it's EmptyOperator)
        "execution_date",  # AF 2 ExternalTaskMarker (if run, as it's EmptyOperator)
        "database",  # BaseSQlOperator
        "parameters",  # SQLCheckOperator, SQLValueCheckOperator and BranchSQLOperator
        "column_mapping",  # SQLColumnCheckOperator
        "pass_value",  # SQLValueCheckOperator
        "tol",  # SQLValueCheckOperator
        "metrics_thresholds",  # SQLIntervalCheckOperator
        "ratio_formula",  # SQLIntervalCheckOperator
        "ignore_zero",  # SQLIntervalCheckOperator
        "min_threshold",  # SQLThresholdCheckOperator
        "max_threshold",  # SQLThresholdCheckOperator
        "follow_task_ids_if_true",  # BranchSQLOperator
        "follow_task_ids_if_false",  # BranchSQLOperator
        "follow_branch",  # BranchSQLOperator
        "preoperator",  # SQLInsertRowsOperator
        "postoperator",  # SQLInsertRowsOperator
        "table_name_with_schema",  # SQLInsertRowsOperator
        "column_names",  # SQLInsertRowsOperator
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
        "operator_provider_version": lambda task: get_operator_provider_version(task),
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


def get_processing_engine_facet() -> dict[str, processing_engine_run.ProcessingEngineRunFacet]:
    from openlineage.client.facet_v2 import processing_engine_run

    return {
        "processing_engine": processing_engine_run.ProcessingEngineRunFacet(
            version=AIRFLOW_VERSION,
            name="Airflow",
            openlineageAdapterVersion=OPENLINEAGE_PROVIDER_VERSION,
        )
    }


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
    dag: DAG | SerializedDAG,
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

    def get_task_duration(ti):
        if ti.duration is not None:
            return ti.duration
        if ti.end_date is not None and ti.start_date is not None:
            return (ti.end_date - ti.start_date).total_seconds()
        # Fallback to 0.0 for tasks with missing timestamps (e.g., skipped/terminated tasks)
        return 0.0

    return {
        "airflowState": AirflowStateRunFacet(
            dagRunState=dag_run_state,
            tasksState={ti.task_id: ti.state for ti in tis},
            tasksDuration={ti.task_id: get_task_duration(ti) for ti in tis},
        )
    }


def _get_tasks_details(dag: DAG | SerializedDAG) -> dict:
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


def _get_task_groups_details(dag: DAG | SerializedDAG) -> dict:
    return {
        tg_id: {
            "parent_group": tg.parent_group.group_id,
            "ui_color": tg.ui_color,
            "ui_fgcolor": tg.ui_fgcolor,
            "ui_label": tg.label,
        }
        for tg_id, tg in dag.task_group_dict.items()
    }


def _emits_ol_events(task: AnyOperator) -> bool:
    config_selective_enabled = is_selective_lineage_enabled(task)
    config_disabled_for_operators = is_operator_disabled(task)

    is_task_schedulable_method = getattr(TaskInstance, "is_task_schedulable", None)  # Added in 3.2.0 #56039
    if is_task_schedulable_method and callable(is_task_schedulable_method):
        is_skipped_as_empty_operator = not is_task_schedulable_method(task)
    else:
        # For older Airflow versions, re-create Airflow core internal logic as
        # empty operators without callbacks/outlets are skipped for optimization by Airflow
        # in airflow.models.taskinstance.TaskInstance._schedule_downstream_tasks or
        # airflow.models.dagrun.DagRun.schedule_tis, depending on Airflow version
        is_skipped_as_empty_operator = all(
            (
                task.inherits_from_empty_operator,
                not getattr(task, "on_execute_callback", None),
                not getattr(task, "on_success_callback", None),
                not task.outlets,
                not (task.inlets and get_base_airflow_version_tuple() >= (3, 0, 2)),  # Added in 3.0.2 #50773
                not (
                    getattr(task, "has_on_execute_callback", None)  # Added in 3.1.0 #54569
                    and get_base_airflow_version_tuple() >= (3, 1, 0)
                ),
                not (
                    getattr(task, "has_on_success_callback", None)  # Added in 3.1.0 #54569
                    and get_base_airflow_version_tuple() >= (3, 1, 0)
                ),
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
        for attr in ["sensitive_variables_fields", "min_length_to_mask", "secret_mask_adapter"]:
            if hasattr(other, attr):
                setattr(instance, attr, getattr(other, attr))
        return instance

    def _should_hide_value_for_key(self, name):
        """Compatibility helper for should_hide_value_for_key across Airflow versions."""
        try:
            return self.should_hide_value_for_key(name)
        except AttributeError:
            # fallback to module level function
            return should_hide_value_for_key(name)

    def _redact(self, item: Redactable, name: str | None, depth: int, max_depth: int, **kwargs) -> Redacted:  # type: ignore[override]
        if AIRFLOW_V_3_0_PLUS:
            # Keep compatibility for Airflow 2.x, remove when Airflow 3.0 is the minimum version
            class AirflowContextDeprecationWarning(UserWarning):
                pass
        else:
            from airflow.utils.context import (  # type: ignore[attr-defined,no-redef]
                AirflowContextDeprecationWarning,
            )

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
                if name and self._should_hide_value_for_key(name):
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
                if is_json_serializable(item) and hasattr(item, "__dict__"):
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
                return super()._redact(item, name, depth, max_depth, **kwargs)
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
            except Exception:
                log.warning(
                    "OpenLineage event emission failed. "
                    "Exception below is being caught but it's printed for visibility. "
                    "This has no impact on actual task execution status.",
                    exc_info=True,
                )

        return wrapper

    return decorator


def get_filtered_unknown_operator_keys(operator: BaseOperator) -> dict:
    not_required_keys = {"dag", "task_group"}
    return {attr: value for attr, value in operator.__dict__.items() if attr not in not_required_keys}


def should_use_external_connection(hook) -> bool:
    # If we're at Airflow 2.10, the execution is process-isolated, so we can safely run those again.
    return True


def translate_airflow_asset(asset: Asset, lineage_context) -> OpenLineageDataset | None:
    """
    Convert an Asset with an AIP-60 compliant URI to an OpenLineageDataset.

    This function returns None if no URI normalizer is defined, no asset converter is found or
    some core Airflow changes are missing and ImportError is raised.
    """
    if AIRFLOW_V_3_0_PLUS:
        from airflow.sdk.definitions.asset import _get_normalized_scheme
    else:
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
