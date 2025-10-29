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

from airflow.providers.google.cloud.openlineage.facets import (
    BigQueryJobRunFacet,
    CloudStorageTransferJobFacet,
    CloudStorageTransferRunFacet,
    DataFusionRunFacet,
)


def test_bigquery_job_run_facet():
    facet = BigQueryJobRunFacet(cached=True, billedBytes=123, properties="some_properties")
    assert facet.cached is True
    assert facet.billedBytes == 123
    assert facet.properties == "some_properties"


def test_cloud_storage_transfer_job_facet():
    facet = CloudStorageTransferJobFacet(
        jobName="transferJobs/123",
        projectId="test-project",
        description="S3 to GCS transfer",
        status="ENABLED",
        sourceBucket="my-s3-bucket",
        sourcePath="data/",
        targetBucket="my-gcs-bucket",
        targetPath="backup/",
        objectConditions={"maxTimeElapsedSinceLastModification": "86400s"},
        transferOptions={"overwriteObjectsAlreadyExistingInSink": True},
        schedule={"scheduleStartDate": {"year": 2025, "month": 9, "day": 17}},
    )

    assert facet.jobName == "transferJobs/123"
    assert facet.projectId == "test-project"
    assert facet.description == "S3 to GCS transfer"
    assert facet.status == "ENABLED"
    assert facet.sourceBucket == "my-s3-bucket"
    assert facet.sourcePath == "data/"
    assert facet.targetBucket == "my-gcs-bucket"
    assert facet.targetPath == "backup/"
    assert facet.objectConditions == {"maxTimeElapsedSinceLastModification": "86400s"}
    assert facet.transferOptions == {"overwriteObjectsAlreadyExistingInSink": True}
    assert facet.schedule == {"scheduleStartDate": {"year": 2025, "month": 9, "day": 17}}


def test_cloud_storage_transfer_run_facet():
    facet = CloudStorageTransferRunFacet(
        jobName="transferJobs/123",
        operationName="transferOperations/abc",
        status="SUCCESS",
        startTime="2025-09-17T10:00:00Z",
        endTime="2025-09-17T10:05:00Z",
        wait=True,
        timeout=3600,
        deferrable=False,
        deleteJobAfterCompletion=True,
    )

    assert facet.jobName == "transferJobs/123"
    assert facet.operationName == "transferOperations/abc"
    assert facet.status == "SUCCESS"
    assert facet.startTime == "2025-09-17T10:00:00Z"
    assert facet.endTime == "2025-09-17T10:05:00Z"
    assert facet.wait is True
    assert facet.timeout == 3600
    assert facet.deferrable is False
    assert facet.deleteJobAfterCompletion is True


def test_datafusion_run_facet():
    facet = DataFusionRunFacet(runId="abc123", runtimeArgs={"arg1": "val1"})

    assert facet.runId == "abc123"
    assert facet.runtimeArgs == {"arg1": "val1"}
