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

from airflow.providers.openlineage.plugins.listener import get_openlineage_listener
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


def _get_transport_information_as_spark_properties() -> dict:
    """Retrieve transport information as Spark properties."""
    transport = get_openlineage_listener().adapter.get_or_create_openlineage_client().transport
    if transport.kind != "http":
        log.info(
            "OpenLineage transport type `%s` does not support automatic "
            "injection of OpenLineage transport information into Spark properties.",
            transport.kind,
        )
        return {}

    properties = {
        "spark.openlineage.transport.type": transport.kind,
        "spark.openlineage.transport.url": transport.url,
        "spark.openlineage.transport.endpoint": transport.endpoint,
        "spark.openlineage.transport.timeoutInMillis": str(
            int(transport.timeout * 1000)  # convert to milliseconds, as required by Spark integration
        ),
    }
    if transport.compression:
        properties["spark.openlineage.transport.compression"] = str(transport.compression)

    if hasattr(transport.config.auth, "api_key") and transport.config.auth.get_bearer():
        properties["spark.openlineage.transport.auth.type"] = "api_key"
        properties["spark.openlineage.transport.auth.apiKey"] = transport.config.auth.get_bearer()

    if hasattr(transport.config, "custom_headers") and transport.config.custom_headers:
        for key, value in transport.config.custom_headers.items():
            properties[f"spark.openlineage.transport.headers.{key}"] = value

    return properties


def _is_parent_job_information_present_in_spark_properties(properties: dict) -> bool:
    """
    Check if any parent job information is present in Spark properties.

    Args:
        properties: Spark properties.

    Returns:
        True if parent job information is present, False otherwise.
    """
    return any(str(key).startswith("spark.openlineage.parent") for key in properties)


def _is_transport_information_present_in_spark_properties(properties: dict) -> bool:
    """
    Check if any transport information is present in Spark properties.

    Args:
        properties: Spark properties.

    Returns:
        True if transport information is present, False otherwise.
    """
    return any(str(key).startswith("spark.openlineage.transport") for key in properties)


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

    return {**properties, **_get_parent_job_information_as_spark_properties(context)}


def inject_transport_information_into_spark_properties(properties: dict, context: Context) -> dict:
    """
    Inject transport information into Spark properties if not already present.

    Args:
        properties: Spark properties.
        context: The context containing task instance information.

    Returns:
        Modified Spark properties with OpenLineage transport information properties injected, if applicable.
    """
    if _is_transport_information_present_in_spark_properties(properties):
        log.info(
            "Some OpenLineage properties with transport information are already present "
            "in Spark properties. Skipping the injection of OpenLineage "
            "transport information into Spark properties."
        )
        return properties

    return {**properties, **_get_transport_information_as_spark_properties()}
