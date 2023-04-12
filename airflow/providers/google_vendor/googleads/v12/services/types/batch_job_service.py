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

from airflow.providers.google_vendor.googleads.v12.enums.types import (
    response_content_type as gage_response_content_type,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import batch_job
from airflow.providers.google_vendor.googleads.v12.services.types import google_ads_service
from google.rpc import status_pb2  # type: ignore


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.services",
    marshal="google.ads.googleads.v12",
    manifest={
        "MutateBatchJobRequest",
        "BatchJobOperation",
        "MutateBatchJobResponse",
        "MutateBatchJobResult",
        "RunBatchJobRequest",
        "AddBatchJobOperationsRequest",
        "AddBatchJobOperationsResponse",
        "ListBatchJobResultsRequest",
        "ListBatchJobResultsResponse",
        "BatchJobResult",
    },
)


class MutateBatchJobRequest(proto.Message):
    r"""Request message for
    [BatchJobService.MutateBatchJob][google.ads.googleads.v12.services.BatchJobService.MutateBatchJob].

    Attributes:
        customer_id (str):
            Required. The ID of the customer for which to
            create a batch job.
        operation (google.ads.googleads.v12.services.types.BatchJobOperation):
            Required. The operation to perform on an
            individual batch job.
    """

    customer_id = proto.Field(proto.STRING, number=1,)
    operation = proto.Field(
        proto.MESSAGE, number=2, message="BatchJobOperation",
    )


class BatchJobOperation(proto.Message):
    r"""A single operation on a batch job.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        create (google.ads.googleads.v12.resources.types.BatchJob):
            Create operation: No resource name is
            expected for the new batch job.

            This field is a member of `oneof`_ ``operation``.
        remove (str):
            Remove operation: The batch job must not have been run. A
            resource name for the removed batch job is expected, in this
            format:

            ``customers/{customer_id}/batchJobs/{batch_job_id}``

            This field is a member of `oneof`_ ``operation``.
    """

    create = proto.Field(
        proto.MESSAGE, number=1, oneof="operation", message=batch_job.BatchJob,
    )
    remove = proto.Field(proto.STRING, number=4, oneof="operation",)


class MutateBatchJobResponse(proto.Message):
    r"""Response message for
    [BatchJobService.MutateBatchJob][google.ads.googleads.v12.services.BatchJobService.MutateBatchJob].

    Attributes:
        result (google.ads.googleads.v12.services.types.MutateBatchJobResult):
            The result for the mutate.
    """

    result = proto.Field(
        proto.MESSAGE, number=1, message="MutateBatchJobResult",
    )


class MutateBatchJobResult(proto.Message):
    r"""The result for the batch job mutate.

    Attributes:
        resource_name (str):
            The resource name of the batch job.
    """

    resource_name = proto.Field(proto.STRING, number=1,)


class RunBatchJobRequest(proto.Message):
    r"""Request message for
    [BatchJobService.RunBatchJob][google.ads.googleads.v12.services.BatchJobService.RunBatchJob].

    Attributes:
        resource_name (str):
            Required. The resource name of the BatchJob
            to run.
    """

    resource_name = proto.Field(proto.STRING, number=1,)


class AddBatchJobOperationsRequest(proto.Message):
    r"""Request message for
    [BatchJobService.AddBatchJobOperations][google.ads.googleads.v12.services.BatchJobService.AddBatchJobOperations].

    Attributes:
        resource_name (str):
            Required. The resource name of the batch job.
        sequence_token (str):
            A token used to enforce sequencing.

            The first AddBatchJobOperations request for a batch job
            should not set sequence_token. Subsequent requests must set
            sequence_token to the value of next_sequence_token received
            in the previous AddBatchJobOperations response.
        mutate_operations (Sequence[google.ads.googleads.v12.services.types.MutateOperation]):
            Required. The list of mutates being added.
            Operations can use negative integers as temp ids
            to signify dependencies between entities created
            in this batch job. For example, a customer with
            id = 1234 can create a campaign and an ad group
            in that same campaign by creating a campaign in
            the first operation with the resource name
            explicitly set to "customers/1234/campaigns/-1",
            and creating an ad group in the second operation
            with the campaign field also set to
            "customers/1234/campaigns/-1".
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    sequence_token = proto.Field(proto.STRING, number=2,)
    mutate_operations = proto.RepeatedField(
        proto.MESSAGE, number=3, message=google_ads_service.MutateOperation,
    )


class AddBatchJobOperationsResponse(proto.Message):
    r"""Response message for
    [BatchJobService.AddBatchJobOperations][google.ads.googleads.v12.services.BatchJobService.AddBatchJobOperations].

    Attributes:
        total_operations (int):
            The total number of operations added so far
            for this batch job.
        next_sequence_token (str):
            The sequence token to be used when calling
            AddBatchJobOperations again if more operations need to be
            added. The next AddBatchJobOperations request must set the
            sequence_token field to the value of this field.
    """

    total_operations = proto.Field(proto.INT64, number=1,)
    next_sequence_token = proto.Field(proto.STRING, number=2,)


class ListBatchJobResultsRequest(proto.Message):
    r"""Request message for
    [BatchJobService.ListBatchJobResults][google.ads.googleads.v12.services.BatchJobService.ListBatchJobResults].

    Attributes:
        resource_name (str):
            Required. The resource name of the batch job
            whose results are being listed.
        page_token (str):
            Token of the page to retrieve. If not specified, the first
            page of results will be returned. Use the value obtained
            from ``next_page_token`` in the previous response in order
            to request the next page of results.
        page_size (int):
            Number of elements to retrieve in a single
            page. When a page request is too large, the
            server may decide to further limit the number of
            returned resources.
        response_content_type (google.ads.googleads.v12.enums.types.ResponseContentTypeEnum.ResponseContentType):
            The response content type setting. Determines
            whether the mutable resource or just the
            resource name should be returned.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    page_token = proto.Field(proto.STRING, number=2,)
    page_size = proto.Field(proto.INT32, number=3,)
    response_content_type = proto.Field(
        proto.ENUM,
        number=4,
        enum=gage_response_content_type.ResponseContentTypeEnum.ResponseContentType,
    )


class ListBatchJobResultsResponse(proto.Message):
    r"""Response message for
    [BatchJobService.ListBatchJobResults][google.ads.googleads.v12.services.BatchJobService.ListBatchJobResults].

    Attributes:
        results (Sequence[google.ads.googleads.v12.services.types.BatchJobResult]):
            The list of rows that matched the query.
        next_page_token (str):
            Pagination token used to retrieve the next page of results.
            Pass the content of this string as the ``page_token``
            attribute of the next request. ``next_page_token`` is not
            returned for the last page.
    """

    @property
    def raw_page(self):
        return self

    results = proto.RepeatedField(
        proto.MESSAGE, number=1, message="BatchJobResult",
    )
    next_page_token = proto.Field(proto.STRING, number=2,)


class BatchJobResult(proto.Message):
    r"""An individual batch job result.

    Attributes:
        operation_index (int):
            Index of the mutate operation.
        mutate_operation_response (google.ads.googleads.v12.services.types.MutateOperationResponse):
            Response for the mutate.
            May be empty if errors occurred.
        status (google.rpc.status_pb2.Status):
            Details of the errors when processing the
            operation.
    """

    operation_index = proto.Field(proto.INT64, number=1,)
    mutate_operation_response = proto.Field(
        proto.MESSAGE,
        number=2,
        message=google_ads_service.MutateOperationResponse,
    )
    status = proto.Field(proto.MESSAGE, number=3, message=status_pb2.Status,)


__all__ = tuple(sorted(__protobuf__.manifest))
