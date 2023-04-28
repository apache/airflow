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
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    conversion_goal_campaign_config as gagr_conversion_goal_campaign_config,
)
from google.protobuf import field_mask_pb2  # type: ignore


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.services",
    marshal="google.ads.googleads.v12",
    manifest={
        "MutateConversionGoalCampaignConfigsRequest",
        "ConversionGoalCampaignConfigOperation",
        "MutateConversionGoalCampaignConfigsResponse",
        "MutateConversionGoalCampaignConfigResult",
    },
)


class MutateConversionGoalCampaignConfigsRequest(proto.Message):
    r"""Request message for
    [ConversionGoalCampaignConfigService.MutateConversionGoalCampaignConfig][].

    Attributes:
        customer_id (str):
            Required. The ID of the customer whose custom
            conversion goals are being modified.
        operations (Sequence[google.ads.googleads.v12.services.types.ConversionGoalCampaignConfigOperation]):
            Required. The list of operations to perform
            on individual conversion goal campaign config.
        validate_only (bool):
            If true, the request is validated but not
            executed. Only errors are returned, not results.
        response_content_type (google.ads.googleads.v12.enums.types.ResponseContentTypeEnum.ResponseContentType):
            The response content type setting. Determines
            whether the mutable resource or just the
            resource name should be returned post mutation.
    """

    customer_id = proto.Field(proto.STRING, number=1,)
    operations = proto.RepeatedField(
        proto.MESSAGE,
        number=2,
        message="ConversionGoalCampaignConfigOperation",
    )
    validate_only = proto.Field(proto.BOOL, number=3,)
    response_content_type = proto.Field(
        proto.ENUM,
        number=4,
        enum=gage_response_content_type.ResponseContentTypeEnum.ResponseContentType,
    )


class ConversionGoalCampaignConfigOperation(proto.Message):
    r"""A single operation (update) on a conversion goal campaign
    config.


    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        update_mask (google.protobuf.field_mask_pb2.FieldMask):
            FieldMask that determines which resource
            fields are modified in an update.
        update (google.ads.googleads.v12.resources.types.ConversionGoalCampaignConfig):
            Update operation: The conversion goal
            campaign config is expected to have a valid
            resource name.

            This field is a member of `oneof`_ ``operation``.
    """

    update_mask = proto.Field(
        proto.MESSAGE, number=2, message=field_mask_pb2.FieldMask,
    )
    update = proto.Field(
        proto.MESSAGE,
        number=1,
        oneof="operation",
        message=gagr_conversion_goal_campaign_config.ConversionGoalCampaignConfig,
    )


class MutateConversionGoalCampaignConfigsResponse(proto.Message):
    r"""Response message for a conversion goal campaign config
    mutate.

    Attributes:
        results (Sequence[google.ads.googleads.v12.services.types.MutateConversionGoalCampaignConfigResult]):
            All results for the mutate.
    """

    results = proto.RepeatedField(
        proto.MESSAGE,
        number=1,
        message="MutateConversionGoalCampaignConfigResult",
    )


class MutateConversionGoalCampaignConfigResult(proto.Message):
    r"""The result for the conversion goal campaign config mutate.

    Attributes:
        resource_name (str):
            Returned for successful operations.
        conversion_goal_campaign_config (google.ads.googleads.v12.resources.types.ConversionGoalCampaignConfig):
            The mutated ConversionGoalCampaignConfig with only mutable
            fields after mutate. The field will only be returned when
            response_content_type is set to "MUTABLE_RESOURCE".
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    conversion_goal_campaign_config = proto.Field(
        proto.MESSAGE,
        number=2,
        message=gagr_conversion_goal_campaign_config.ConversionGoalCampaignConfig,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
