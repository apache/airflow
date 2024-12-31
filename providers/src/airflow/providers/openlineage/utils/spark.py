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

from airflow.providers.openlineage.plugins.macros import (
    lineage_job_name,
    lineage_job_namespace,
    lineage_run_id,
)

if TYPE_CHECKING:
    from airflow.utils.context import Context

log = logging.getLogger(__name__)


def _get_parent_job_information_as_spark_properties(context: Context) -> dict:
    """
    Retrieve parent job information as Spark properties.

    Args:
        context: The context containing task instance information.

    Returns:
        Spark properties with the parent job information.
    """
    ti = context["ti"]
    return {
        "spark.openlineage.parentJobNamespace": lineage_job_namespace(),
        "spark.openlineage.parentJobName": lineage_job_name(ti),  # type: ignore[arg-type]
        "spark.openlineage.parentRunId": lineage_run_id(ti),  # type: ignore[arg-type]
    }


def _is_parent_job_information_present_in_spark_properties(properties: dict) -> bool:
    """
    Check if any parent job information is present in Spark properties.

    Args:
        properties: Spark properties.

    Returns:
        True if parent job information is present, False otherwise.
    """
    return any(str(key).startswith("spark.openlineage.parent") for key in properties)


def inject_parent_job_information_into_spark_properties(properties: dict, context: Context) -> dict:
    """
    Inject parent job information into Spark properties if not already present.

    Args:
        properties: Spark properties.
        context: The context containing task instance information.

    Returns:
        Modified Spark properties with OpenLineage parent job information properties injected, if applicable.
    """
    if _is_parent_job_information_present_in_spark_properties(properties):
        log.info(
            "Some OpenLineage properties with parent job information are already present "
            "in Spark properties. Skipping the injection of OpenLineage "
            "parent job information into Spark properties."
        )
        return properties

    ol_parent_job_properties = _get_parent_job_information_as_spark_properties(context)
    return {**properties, **ol_parent_job_properties}
