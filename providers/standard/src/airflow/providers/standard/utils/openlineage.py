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

import logging
from typing import TYPE_CHECKING

from airflow.exceptions import AirflowOptionalProviderFeatureException
from airflow.providers.common.compat.openlineage.check import require_openlineage_version

if TYPE_CHECKING:
    from airflow.models import TaskInstance
    from airflow.sdk.types import RuntimeTaskInstanceProtocol as RuntimeTI

log = logging.getLogger(__name__)

OPENLINEAGE_PROVIDER_MIN_VERSION = "2.8.0"


def _is_openlineage_provider_accessible() -> bool:
    """
    Check if the OpenLineage provider is accessible.

    This function attempts to import the necessary OpenLineage modules and checks if the provider
    is enabled and the listener is available.

    Returns:
        bool: True if the OpenLineage provider is accessible, False otherwise.
    """
    try:
        from airflow.providers.openlineage.conf import is_disabled
        from airflow.providers.openlineage.plugins.listener import get_openlineage_listener
    except (ImportError, AttributeError):
        log.debug("OpenLineage provider could not be imported.")
        return False

    if is_disabled():
        log.debug("OpenLineage provider is disabled.")
        return False

    if not get_openlineage_listener():
        log.debug("OpenLineage listener could not be found.")
        return False

    return True


@require_openlineage_version(provider_min_version=OPENLINEAGE_PROVIDER_MIN_VERSION)
def _get_openlineage_parent_info(ti: TaskInstance | RuntimeTI) -> dict[str, str]:
    """Get OpenLineage metadata about the parent task."""
    from airflow.providers.openlineage.plugins.macros import (
        lineage_job_name,
        lineage_job_namespace,
        lineage_root_job_name,
        lineage_root_job_namespace,
        lineage_root_run_id,
        lineage_run_id,
    )

    return {
        "parentRunId": lineage_run_id(ti),
        "parentJobName": lineage_job_name(ti),
        "parentJobNamespace": lineage_job_namespace(),
        "rootParentRunId": lineage_root_run_id(ti),
        "rootParentJobName": lineage_root_job_name(ti),
        "rootParentJobNamespace": lineage_root_job_namespace(ti),
    }


def _inject_openlineage_parent_info_to_dagrun_conf(
    dr_conf: dict | None, ol_parent_info: dict[str, str]
) -> dict:
    """
    Safely inject OpenLineage parent and root run metadata into a DAG run configuration.

    This function adds parent and root job/run identifiers derived from the given TaskInstance into the
    `openlineage` section of the DAG run configuration. If an `openlineage` key already exists, it is
    preserved and extended, but no existing parent or root identifiers are overwritten.

    The function performs several safety checks:
    - If conf is not a dictionary or contains a non-dict `openlineage` section, conf is returned unmodified.
    - If `openlineage` section contains any parent/root lineage identifiers, conf is returned unmodified.

    Args:
        dr_conf: The original DAG run configuration dictionary or None.
        ol_parent_info: OpenLineage metadata about the parent task

    Returns:
        A modified DAG run conf with injected OpenLineage parent and root metadata,
        or the original conf if injection is not possible.
    """
    current_ol_dr_conf = {}
    if isinstance(dr_conf, dict) and dr_conf.get("openlineage"):
        current_ol_dr_conf = dr_conf["openlineage"]
        if not isinstance(current_ol_dr_conf, dict):
            log.warning(
                "Existing 'openlineage' section of DagRun conf is not a dictionary; "
                "skipping injection of parent metadata."
            )
            return dr_conf
        forbidden_keys = (
            "parentRunId",
            "parentJobName",
            "parentJobNamespace",
            "rootParentRunId",
            "rootJobName",
            "rootJobNamespace",
        )

        if existing := [k for k in forbidden_keys if k in current_ol_dr_conf]:
            log.warning(
                "'openlineage' section of DagRun conf already contains parent or root "
                "identifiers: `%s`; skipping injection to avoid overwriting existing values.",
                ", ".join(existing),
            )
            return dr_conf

    return {**(dr_conf or {}), **{"openlineage": {**ol_parent_info, **current_ol_dr_conf}}}


def safe_inject_openlineage_properties_into_dagrun_conf(
    dr_conf: dict | None, ti: TaskInstance | RuntimeTI | None
) -> dict | None:
    """
    Safely inject OpenLineage parent task metadata into a DAG run conf.

    This function checks whether the OpenLineage provider is accessible and supports parent information
    injection. If so, it enriches the DAG run conf with OpenLineage metadata about the parent task
    to improve lineage tracking. The function does not modify other conf fields, will not overwrite
    any existing content, and safely returns the original configuration if OpenLineage is unavailable,
    unsupported, or an error occurs during injection.

    :param dr_conf: The original DAG run configuration dictionary.
    :param ti: The TaskInstance whose metadata may be injected.

    :return: A potentially enriched DAG run conf with OpenLineage parent information,
        or the original conf if injection was skipped or failed.
    """
    try:
        if ti is None:
            log.debug("Task instance not provided - dagrun conf not modified.")
            return dr_conf

        if not _is_openlineage_provider_accessible():
            log.debug("OpenLineage provider not accessible - dagrun conf not modified.")
            return dr_conf

        ol_parent_info = _get_openlineage_parent_info(ti=ti)

        log.info("Injecting openlineage parent task information into dagrun conf.")
        new_conf = _inject_openlineage_parent_info_to_dagrun_conf(
            dr_conf=dr_conf.copy() if isinstance(dr_conf, dict) else dr_conf,
            ol_parent_info=ol_parent_info,
        )
        return new_conf
    except AirflowOptionalProviderFeatureException:
        log.info(
            "Current OpenLineage provider version doesn't support parent information in "
            "the DagRun conf. Upgrade `apache-airflow-providers-openlineage>=%s` to use this feature. "
            "DagRun conf has not been modified by OpenLineage.",
            OPENLINEAGE_PROVIDER_MIN_VERSION,
        )
        return dr_conf
    except Exception as e:
        log.warning(
            "An error occurred while trying to inject OpenLineage information into dagrun conf. "
            "DagRun conf has not been modified by OpenLineage. Error: %s",
            str(e),
        )
        log.debug("Error details: ", exc_info=e)
        return dr_conf
