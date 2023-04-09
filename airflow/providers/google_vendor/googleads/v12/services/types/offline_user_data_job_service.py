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

from airflow.providers.google_vendor.googleads.v12.common.types import offline_user_data
from airflow.providers.google_vendor.googleads.v12.resources.types import offline_user_data_job
from google.rpc import status_pb2  # type: ignore


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.services",
    marshal="google.ads.googleads.v12",
    manifest={
        "CreateOfflineUserDataJobRequest",
        "CreateOfflineUserDataJobResponse",
        "RunOfflineUserDataJobRequest",
        "AddOfflineUserDataJobOperationsRequest",
        "OfflineUserDataJobOperation",
        "AddOfflineUserDataJobOperationsResponse",
    },
)


class CreateOfflineUserDataJobRequest(proto.Message):
    r"""Request message for
    [OfflineUserDataJobService.CreateOfflineUserDataJob][google.ads.googleads.v12.services.OfflineUserDataJobService.CreateOfflineUserDataJob].

    Attributes:
        customer_id (str):
            Required. The ID of the customer for which to
            create an offline user data job.
        job (google.ads.googleads.v12.resources.types.OfflineUserDataJob):
            Required. The offline user data job to be
            created.
        validate_only (bool):
            If true, the request is validated but not
            executed. Only errors are returned, not results.
        enable_match_rate_range_preview (bool):
            If true, match rate range for the offline
            user data job is calculated and made available
            in the resource.
    """

    customer_id = proto.Field(proto.STRING, number=1,)
    job = proto.Field(
        proto.MESSAGE,
        number=2,
        message=offline_user_data_job.OfflineUserDataJob,
    )
    validate_only = proto.Field(proto.BOOL, number=3,)
    enable_match_rate_range_preview = proto.Field(proto.BOOL, number=5,)


class CreateOfflineUserDataJobResponse(proto.Message):
    r"""Response message for
    [OfflineUserDataJobService.CreateOfflineUserDataJob][google.ads.googleads.v12.services.OfflineUserDataJobService.CreateOfflineUserDataJob].

    Attributes:
        resource_name (str):
            The resource name of the OfflineUserDataJob.
    """

    resource_name = proto.Field(proto.STRING, number=1,)


class RunOfflineUserDataJobRequest(proto.Message):
    r"""Request message for
    [OfflineUserDataJobService.RunOfflineUserDataJob][google.ads.googleads.v12.services.OfflineUserDataJobService.RunOfflineUserDataJob].

    Attributes:
        resource_name (str):
            Required. The resource name of the
            OfflineUserDataJob to run.
        validate_only (bool):
            If true, the request is validated but not
            executed. Only errors are returned, not results.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    validate_only = proto.Field(proto.BOOL, number=2,)


class AddOfflineUserDataJobOperationsRequest(proto.Message):
    r"""Request message for
    [OfflineUserDataJobService.AddOfflineUserDataJobOperations][google.ads.googleads.v12.services.OfflineUserDataJobService.AddOfflineUserDataJobOperations].

    Attributes:
        resource_name (str):
            Required. The resource name of the
            OfflineUserDataJob.
        enable_partial_failure (bool):
            True to enable partial failure for the
            offline user data job.

            This field is a member of `oneof`_ ``_enable_partial_failure``.
        enable_warnings (bool):
            True to enable warnings for the offline user
            data job. When enabled, a warning will not block
            the OfflineUserDataJobOperation, and will also
            return warning messages about malformed field
            values.

            This field is a member of `oneof`_ ``_enable_warnings``.
        operations (Sequence[google.ads.googleads.v12.services.types.OfflineUserDataJobOperation]):
            Required. The list of operations to be done.
        validate_only (bool):
            If true, the request is validated but not
            executed. Only errors are returned, not results.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    enable_partial_failure = proto.Field(proto.BOOL, number=4, optional=True,)
    enable_warnings = proto.Field(proto.BOOL, number=6, optional=True,)
    operations = proto.RepeatedField(
        proto.MESSAGE, number=3, message="OfflineUserDataJobOperation",
    )
    validate_only = proto.Field(proto.BOOL, number=5,)


class OfflineUserDataJobOperation(proto.Message):
    r"""Operation to be made for the
    AddOfflineUserDataJobOperationsRequest.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        create (google.ads.googleads.v12.common.types.UserData):
            Add the provided data to the transaction.
            Data cannot be retrieved after being uploaded.

            This field is a member of `oneof`_ ``operation``.
        remove (google.ads.googleads.v12.common.types.UserData):
            Remove the provided data from the
            transaction. Data cannot be retrieved after
            being uploaded.

            This field is a member of `oneof`_ ``operation``.
        remove_all (bool):
            Remove all previously provided data. This is
            only supported for Customer Match.

            This field is a member of `oneof`_ ``operation``.
    """

    create = proto.Field(
        proto.MESSAGE,
        number=1,
        oneof="operation",
        message=offline_user_data.UserData,
    )
    remove = proto.Field(
        proto.MESSAGE,
        number=2,
        oneof="operation",
        message=offline_user_data.UserData,
    )
    remove_all = proto.Field(proto.BOOL, number=3, oneof="operation",)


class AddOfflineUserDataJobOperationsResponse(proto.Message):
    r"""Response message for
    [OfflineUserDataJobService.AddOfflineUserDataJobOperations][google.ads.googleads.v12.services.OfflineUserDataJobService.AddOfflineUserDataJobOperations].

    Attributes:
        partial_failure_error (google.rpc.status_pb2.Status):
            Errors that pertain to operation failures in the partial
            failure mode. Returned only when partial_failure = true and
            all errors occur inside the operations. If any errors occur
            outside the operations (for example, auth errors), we return
            an RPC level error.
        warning (google.rpc.status_pb2.Status):
            Non blocking errors that pertain to operation failures in
            the warnings mode. Returned only when enable_warnings =
            true.
    """

    partial_failure_error = proto.Field(
        proto.MESSAGE, number=1, message=status_pb2.Status,
    )
    warning = proto.Field(proto.MESSAGE, number=2, message=status_pb2.Status,)


__all__ = tuple(sorted(__protobuf__.manifest))
