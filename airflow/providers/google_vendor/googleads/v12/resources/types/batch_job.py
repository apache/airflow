# -*- coding: utf-8 -*-
# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import proto  # type: ignore

from airflow.providers.google_vendor.googleads.v12.enums.types import batch_job_status


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"BatchJob",},
)


class BatchJob(proto.Message):
    r"""A list of mutates being processed asynchronously. The mutates
    are uploaded by the user. The mutates themselves aren't readable
    and the results of the job can only be read using
    BatchJobService.ListBatchJobResults.

    Attributes:
        resource_name (str):
            Immutable. The resource name of the batch job. Batch job
            resource names have the form:

            ``customers/{customer_id}/batchJobs/{batch_job_id}``
        id (int):
            Output only. ID of this batch job.

            This field is a member of `oneof`_ ``_id``.
        next_add_sequence_token (str):
            Output only. The next sequence token to use
            when adding operations. Only set when the batch
            job status is PENDING.

            This field is a member of `oneof`_ ``_next_add_sequence_token``.
        metadata (google.ads.googleads.v12.resources.types.BatchJob.BatchJobMetadata):
            Output only. Contains additional information
            about this batch job.
        status (google.ads.googleads.v12.enums.types.BatchJobStatusEnum.BatchJobStatus):
            Output only. Status of this batch job.
        long_running_operation (str):
            Output only. The resource name of the
            long-running operation that can be used to poll
            for completion. Only set when the batch job
            status is RUNNING or DONE.

            This field is a member of `oneof`_ ``_long_running_operation``.
    """

    class BatchJobMetadata(proto.Message):
        r"""Additional information about the batch job. This message is
        also used as metadata returned in batch job Long Running
        Operations.

        Attributes:
            creation_date_time (str):
                Output only. The time when this batch job was
                created. Formatted as yyyy-mm-dd hh:mm:ss.
                Example: "2018-03-05 09:15:00".

                This field is a member of `oneof`_ ``_creation_date_time``.
            start_date_time (str):
                Output only. The time when this batch job
                started running. Formatted as yyyy-mm-dd
                hh:mm:ss. Example: "2018-03-05 09:15:30".

                This field is a member of `oneof`_ ``_start_date_time``.
            completion_date_time (str):
                Output only. The time when this batch job was
                completed. Formatted as yyyy-MM-dd HH:mm:ss.
                Example: "2018-03-05 09:16:00".

                This field is a member of `oneof`_ ``_completion_date_time``.
            estimated_completion_ratio (float):
                Output only. The fraction (between 0.0 and
                1.0) of mutates that have been processed. This
                is empty if the job hasn't started running yet.

                This field is a member of `oneof`_ ``_estimated_completion_ratio``.
            operation_count (int):
                Output only. The number of mutate operations
                in the batch job.

                This field is a member of `oneof`_ ``_operation_count``.
            executed_operation_count (int):
                Output only. The number of mutate operations
                executed by the batch job. Present only if the
                job has started running.

                This field is a member of `oneof`_ ``_executed_operation_count``.
        """

        creation_date_time = proto.Field(proto.STRING, number=8, optional=True,)
        start_date_time = proto.Field(proto.STRING, number=7, optional=True,)
        completion_date_time = proto.Field(
            proto.STRING, number=9, optional=True,
        )
        estimated_completion_ratio = proto.Field(
            proto.DOUBLE, number=10, optional=True,
        )
        operation_count = proto.Field(proto.INT64, number=11, optional=True,)
        executed_operation_count = proto.Field(
            proto.INT64, number=12, optional=True,
        )

    resource_name = proto.Field(proto.STRING, number=1,)
    id = proto.Field(proto.INT64, number=7, optional=True,)
    next_add_sequence_token = proto.Field(
        proto.STRING, number=8, optional=True,
    )
    metadata = proto.Field(proto.MESSAGE, number=4, message=BatchJobMetadata,)
    status = proto.Field(
        proto.ENUM,
        number=5,
        enum=batch_job_status.BatchJobStatusEnum.BatchJobStatus,
    )
    long_running_operation = proto.Field(proto.STRING, number=9, optional=True,)


__all__ = tuple(sorted(__protobuf__.manifest))
