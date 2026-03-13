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
    from openlineage.client.generated.base import JobFacet, RunFacet

try:
    try:
        from openlineage.client.generated.base import RunFacet
    except ImportError:  # Old OpenLineage client is used
        from openlineage.client.facet import BaseFacet as RunFacet  # type: ignore[assignment]
    try:
        from openlineage.client.generated.base import JobFacet
    except ImportError:  # Old OpenLineage client is used
        from openlineage.client.facet import BaseFacet as JobFacet  # type: ignore[assignment]

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

    @define
    class CloudStorageTransferJobFacet(JobFacet):
        """
        Facet representing a Cloud Storage Transfer Service job configuration.

        :param jobName: Unique name of the transfer job.
        :param projectId: GCP project where the transfer job is defined.
        :param description: User-provided description of the transfer job.
        :param status: Current status of the transfer job (e.g. "ENABLED", "DISABLED").
        :param sourceBucket: Name of the source bucket (e.g. AWS S3).
        :param sourcePath: Prefix/path inside the source bucket.
        :param targetBucket: Name of the destination bucket (e.g. GCS).
        :param targetPath: Prefix/path inside the destination bucket.
        :param objectConditions: Object selection rules (e.g. include/exclude prefixes).
        :param transferOptions: Transfer options, such as overwrite behavior or whether to delete objects
        from the source after transfer.
        :param schedule: Schedule for the transfer job (if recurring).
        """

        jobName: str | None = field(default=None)
        projectId: str | None = field(default=None)
        description: str | None = field(default=None)
        status: str | None = field(default=None)
        sourceBucket: str | None = field(default=None)
        sourcePath: str | None = field(default=None)
        targetBucket: str | None = field(default=None)
        targetPath: str | None = field(default=None)
        objectConditions: dict | None = field(default=None)
        transferOptions: dict | None = field(default=None)
        schedule: dict | None = field(default=None)

        @staticmethod
        def _get_schema() -> str:
            return (
                "https://raw.githubusercontent.com/apache/airflow/"
                f"providers-google/{provider_version}/airflow/providers/google/"
                "openlineage/CloudStorageTransferJobFacet.json"
            )

    @define
    class CloudStorageTransferRunFacet(RunFacet):
        """
        Facet representing a Cloud Storage Transfer Service job execution run.

        :param jobName: Name of the transfer job being executed.
        :param operationName: Name of the specific transfer operation instance.
        :param status: Current status of the operation (e.g. "IN_PROGRESS", "SUCCESS", "FAILED").
        :param startTime: Time when the transfer job execution started (ISO 8601 format).
        :param endTime: Time when the transfer job execution finished (ISO 8601 format).
        :param wait: Whether the operator waits for the job to complete before finishing.
        :param timeout: Timeout (in seconds) for the transfer run to complete.
        :param deferrable: Whether the operator defers execution until job completion.
        :param deleteJobAfterCompletion: Whether the operator deletes the transfer job after the run completes.
        """

        jobName: str | None = field(default=None)
        operationName: str | None = field(default=None)
        status: str | None = field(default=None)
        startTime: str | None = field(default=None)
        endTime: str | None = field(default=None)
        wait: bool = field(default=True)
        timeout: float | None = field(default=None)
        deferrable: bool = field(default=False)
        deleteJobAfterCompletion: bool = field(default=False)

        @staticmethod
        def _get_schema() -> str:
            return (
                "https://raw.githubusercontent.com/apache/airflow/"
                f"providers-google/{provider_version}/airflow/providers/google/"
                "openlineage/CloudStorageTransferRunFacet.json"
            )

    @define
    class DataFusionRunFacet(RunFacet):
        """
        Facet that represents relevant details of a Cloud Data Fusion pipeline run.

        :param runId: The pipeline execution id.
        :param runtimeArgs: Runtime arguments passed to the pipeline.
        """

        runId: str | None = field(default=None)
        runtimeArgs: dict[str, str] | None = field(default=None)

        @staticmethod
        def _get_schema() -> str:
            return (
                "https://raw.githubusercontent.com/apache/airflow/"
                f"providers-google/{provider_version}/airflow/providers/google/"
                "openlineage/DataFusionRunFacet.json"
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
    CloudStorageTransferJobFacet = create_no_op  # type: ignore[misc, assignment]
    CloudStorageTransferRunFacet = create_no_op  # type: ignore[misc, assignment]
    DataFusionRunFacet = create_no_op  # type: ignore[misc, assignment]
