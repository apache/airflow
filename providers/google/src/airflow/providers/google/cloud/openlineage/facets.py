#
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

from typing import TYPE_CHECKING

from attr import define, field

from airflow.providers.google import __version__ as provider_version

if TYPE_CHECKING:
    from openlineage.client.generated.base import RunFacet

try:
    try:
        from openlineage.client.generated.base import RunFacet
    except ImportError:  # Old OpenLineage client is used
        from openlineage.client.facet import BaseFacet as RunFacet  # type: ignore[assignment]

    @define
    class BigQueryJobRunFacet(RunFacet):
        """
        Facet that represents relevant statistics of bigquery run.

        :param cached: BigQuery caches query results. Rest of the statistics will not be provided for cached queries.
        :param billedBytes: How many bytes BigQuery bills for.
        :param properties: Full property tree of BigQUery run.
        """

        cached: bool
        billedBytes: int | None = field(default=None)
        properties: str | None = field(default=None)

        @staticmethod
        def _get_schema() -> str:
            return (
                "https://raw.githubusercontent.com/apache/airflow/"
                f"providers-google/{provider_version}/airflow/providers/google/"
                "openlineage/BigQueryJobRunFacet.json"
            )
except ImportError:  # OpenLineage is not available

    def create_no_op(*_, **__) -> None:
        """
        Create a no-op placeholder.

        This function creates and returns a None value, used as a placeholder when the OpenLineage client
        library is available. It represents an action that has no effect.
        """
        return None

    BigQueryJobRunFacet = create_no_op  # type: ignore[misc, assignment]
