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

from airflow.providers.google_vendor.googleads.v12.common.types import policy
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    response_content_type as gage_response_content_type,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import ad as gagr_ad
from google.protobuf import field_mask_pb2  # type: ignore
from google.rpc import status_pb2  # type: ignore


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.services",
    marshal="google.ads.googleads.v12",
    manifest={
        "GetAdRequest",
        "MutateAdsRequest",
        "AdOperation",
        "MutateAdsResponse",
        "MutateAdResult",
    },
)


class GetAdRequest(proto.Message):
    r"""Request message for
    [AdService.GetAd][google.ads.googleads.v12.services.AdService.GetAd].

    Attributes:
        resource_name (str):
            Required. The resource name of the ad to
            fetch.
    """

    resource_name = proto.Field(proto.STRING, number=1,)


class MutateAdsRequest(proto.Message):
    r"""Request message for
    [AdService.MutateAds][google.ads.googleads.v12.services.AdService.MutateAds].

    Attributes:
        customer_id (str):
            Required. The ID of the customer whose ads
            are being modified.
        operations (Sequence[google.ads.googleads.v12.services.types.AdOperation]):
            Required. The list of operations to perform
            on individual ads.
        partial_failure (bool):
            If true, successful operations will be
            carried out and invalid operations will return
            errors. If false, all operations will be carried
            out in one transaction if and only if they are
            all valid. Default is false.
        response_content_type (google.ads.googleads.v12.enums.types.ResponseContentTypeEnum.ResponseContentType):
            The response content type setting. Determines
            whether the mutable resource or just the
            resource name should be returned post mutation.
        validate_only (bool):
            If true, the request is validated but not
            executed. Only errors are returned, not results.
    """

    customer_id = proto.Field(proto.STRING, number=1,)
    operations = proto.RepeatedField(
        proto.MESSAGE, number=2, message="AdOperation",
    )
    partial_failure = proto.Field(proto.BOOL, number=4,)
    response_content_type = proto.Field(
        proto.ENUM,
        number=5,
        enum=gage_response_content_type.ResponseContentTypeEnum.ResponseContentType,
    )
    validate_only = proto.Field(proto.BOOL, number=3,)


class AdOperation(proto.Message):
    r"""A single update operation on an ad.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        update_mask (google.protobuf.field_mask_pb2.FieldMask):
            FieldMask that determines which resource
            fields are modified in an update.
        policy_validation_parameter (google.ads.googleads.v12.common.types.PolicyValidationParameter):
            Configuration for how policies are validated.
        update (google.ads.googleads.v12.resources.types.Ad):
            Update operation: The ad is expected to have a valid
            resource name in this format:

            ``customers/{customer_id}/ads/{ad_id}``

            This field is a member of `oneof`_ ``operation``.
    """

    update_mask = proto.Field(
        proto.MESSAGE, number=2, message=field_mask_pb2.FieldMask,
    )
    policy_validation_parameter = proto.Field(
        proto.MESSAGE, number=3, message=policy.PolicyValidationParameter,
    )
    update = proto.Field(
        proto.MESSAGE, number=1, oneof="operation", message=gagr_ad.Ad,
    )


class MutateAdsResponse(proto.Message):
    r"""Response message for an ad mutate.

    Attributes:
        partial_failure_error (google.rpc.status_pb2.Status):
            Errors that pertain to operation failures in the partial
            failure mode. Returned only when partial_failure = true and
            all errors occur inside the operations. If any errors occur
            outside the operations (for example, auth errors), we return
            an RPC level error.
        results (Sequence[google.ads.googleads.v12.services.types.MutateAdResult]):
            All results for the mutate.
    """

    partial_failure_error = proto.Field(
        proto.MESSAGE, number=3, message=status_pb2.Status,
    )
    results = proto.RepeatedField(
        proto.MESSAGE, number=2, message="MutateAdResult",
    )


class MutateAdResult(proto.Message):
    r"""The result for the ad mutate.

    Attributes:
        resource_name (str):
            The resource name returned for successful
            operations.
        ad (google.ads.googleads.v12.resources.types.Ad):
            The mutated ad with only mutable fields after mutate. The
            field will only be returned when response_content_type is
            set to "MUTABLE_RESOURCE".
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    ad = proto.Field(proto.MESSAGE, number=2, message=gagr_ad.Ad,)


__all__ = tuple(sorted(__protobuf__.manifest))
