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

log = logging.getLogger(__name__)

if TYPE_CHECKING:
    from airflow.providers.openlineage.utils.spark import (
        inject_parent_job_information_into_spark_properties,
        inject_transport_information_into_spark_properties,
    )
else:
    try:
        from airflow.providers.openlineage.utils.spark import (
            inject_parent_job_information_into_spark_properties,
            inject_transport_information_into_spark_properties,
        )
    except ImportError:
        try:
            from airflow.providers.openlineage.plugins.macros import (
                lineage_job_name,
                lineage_job_namespace,
                lineage_run_id,
            )
        except ImportError:

            def inject_parent_job_information_into_spark_properties(properties: dict, context) -> dict:
                log.warning(
                    "Could not import `airflow.providers.openlineage.plugins.macros`."
                    "Skipping the injection of OpenLineage parent job information into Spark properties."
                )
                return properties

        else:

            def inject_parent_job_information_into_spark_properties(properties: dict, context) -> dict:
                if any(str(key).startswith("spark.openlineage.parent") for key in properties):
                    log.info(
                        "Some OpenLineage properties with parent job information are already present "
                        "in Spark properties. Skipping the injection of OpenLineage "
                        "parent job information into Spark properties."
                    )
                    return properties

                ti = context["ti"]
                ol_parent_job_properties = {
                    "spark.openlineage.parentJobNamespace": lineage_job_namespace(),
                    "spark.openlineage.parentJobName": lineage_job_name(ti),
                    "spark.openlineage.parentRunId": lineage_run_id(ti),
                }
                return {**properties, **ol_parent_job_properties}

        try:
            from airflow.providers.openlineage.plugins.listener import get_openlineage_listener
        except ImportError:

            def inject_transport_information_into_spark_properties(properties: dict, context) -> dict:
                log.warning(
                    "Could not import `airflow.providers.openlineage.plugins.listener`."
                    "Skipping the injection of OpenLineage transport information into Spark properties."
                )
                return properties

        else:

            def inject_transport_information_into_spark_properties(properties: dict, context) -> dict:
                if any(str(key).startswith("spark.openlineage.transport") for key in properties):
                    log.info(
                        "Some OpenLineage properties with transport information are already present "
                        "in Spark properties. Skipping the injection of OpenLineage "
                        "transport information into Spark properties."
                    )
                    return properties

                def _get_transport_information_as_spark_properties() -> dict:
                    """Retrieve transport information as Spark properties."""

                    def _get_transport_information(tp) -> dict:
                        props = {
                            "type": tp.kind,
                            "url": tp.url,
                            "endpoint": tp.endpoint,
                            "timeoutInMillis": str(
                                int(tp.timeout * 1000)
                                # convert to milliseconds, as required by Spark integration
                            ),
                        }
                        if hasattr(tp, "compression") and tp.compression:
                            props["compression"] = str(tp.compression)

                        if hasattr(tp.config.auth, "api_key") and tp.config.auth.get_bearer():
                            props["auth.type"] = "api_key"
                            props["auth.apiKey"] = tp.config.auth.get_bearer()

                        if hasattr(tp.config, "custom_headers") and tp.config.custom_headers:
                            for key, value in tp.config.custom_headers.items():
                                props[f"headers.{key}"] = value
                        return props

                    def _format_transport(props: dict, transport: dict, name: str | None):
                        for key, value in transport.items():
                            if name:
                                props[f"spark.openlineage.transport.transports.{name}.{key}"] = value
                            else:
                                props[f"spark.openlineage.transport.{key}"] = value
                        return props

                    transport = (
                        get_openlineage_listener().adapter.get_or_create_openlineage_client().transport
                    )

                    if transport.kind == "composite":
                        http_transports = {}
                        for nested_transport in transport.transports:
                            if nested_transport.kind == "http":
                                http_transports[nested_transport.name] = _get_transport_information(
                                    nested_transport
                                )
                            else:
                                name = (
                                    nested_transport.name if hasattr(nested_transport, "name") else "no-name"
                                )
                                log.info(
                                    "OpenLineage transport type `%s` with name `%s` is not supported in composite transport.",
                                    nested_transport.kind,
                                    name,
                                )
                        if len(http_transports) == 0:
                            log.warning(
                                "OpenLineage transport type `composite` does not contain http transport. Skipping "
                                "injection of OpenLineage transport information into Spark properties.",
                            )
                            return {}
                        props = {
                            "spark.openlineage.transport.type": "composite",
                            "spark.openlineage.transport.continueOnFailure": str(
                                transport.config.continue_on_failure
                            ),
                        }
                        for name, http_transport in http_transports.items():
                            props = _format_transport(props, http_transport, name)
                        return props

                    elif transport.kind == "http":
                        return _format_transport({}, _get_transport_information(transport), None)

                    log.info(
                        "OpenLineage transport type `%s` does not support automatic "
                        "injection of OpenLineage transport information into Spark properties.",
                        transport.kind,
                    )
                    return {}

                return {**properties, **_get_transport_information_as_spark_properties()}


__all__ = [
    "inject_parent_job_information_into_spark_properties",
    "inject_transport_information_into_spark_properties",
]
