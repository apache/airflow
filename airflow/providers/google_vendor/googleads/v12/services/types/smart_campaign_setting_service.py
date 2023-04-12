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
    smart_campaign_setting as gagr_smart_campaign_setting,
)
from google.protobuf import field_mask_pb2  # type: ignore
from google.rpc import status_pb2  # type: ignore


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.services",
    marshal="google.ads.googleads.v12",
    manifest={
        "MutateSmartCampaignSettingsRequest",
        "SmartCampaignSettingOperation",
        "MutateSmartCampaignSettingsResponse",
        "MutateSmartCampaignSettingResult",
    },
)


class MutateSmartCampaignSettingsRequest(proto.Message):
    r"""Request message for
    [SmartCampaignSettingService.MutateSmartCampaignSetting][].

    Attributes:
        customer_id (str):
            Required. The ID of the customer whose Smart
            campaign settings are being modified.
        operations (Sequence[google.ads.googleads.v12.services.types.SmartCampaignSettingOperation]):
            Required. The list of operations to perform
            on individual Smart campaign settings.
        partial_failure (bool):
            If true, successful operations will be
            carried out and invalid operations will return
            errors. If false, all operations will be carried
            out in one transaction if and only if they are
            all valid. Default is false.
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
        proto.MESSAGE, number=2, message="SmartCampaignSettingOperation",
    )
    partial_failure = proto.Field(proto.BOOL, number=3,)
    validate_only = proto.Field(proto.BOOL, number=4,)
    response_content_type = proto.Field(
        proto.ENUM,
        number=5,
        enum=gage_response_content_type.ResponseContentTypeEnum.ResponseContentType,
    )


class SmartCampaignSettingOperation(proto.Message):
    r"""A single operation to update Smart campaign settings for a
    campaign.

    Attributes:
        update (google.ads.googleads.v12.resources.types.SmartCampaignSetting):
            Update operation: The Smart campaign setting
            must specify a valid resource name.
        update_mask (google.protobuf.field_mask_pb2.FieldMask):
            FieldMask that determines which resource
            fields are modified in an update.
    """

    update = proto.Field(
        proto.MESSAGE,
        number=1,
        message=gagr_smart_campaign_setting.SmartCampaignSetting,
    )
    update_mask = proto.Field(
        proto.MESSAGE, number=2, message=field_mask_pb2.FieldMask,
    )


class MutateSmartCampaignSettingsResponse(proto.Message):
    r"""Response message for campaign mutate.

    Attributes:
        partial_failure_error (google.rpc.status_pb2.Status):
            Errors that pertain to operation failures in the partial
            failure mode. Returned only when partial_failure = true and
            all errors occur inside the operations. If any errors occur
            outside the operations (for example, auth errors), we return
            an RPC level error.
        results (Sequence[google.ads.googleads.v12.services.types.MutateSmartCampaignSettingResult]):
            All results for the mutate.
    """

    partial_failure_error = proto.Field(
        proto.MESSAGE, number=1, message=status_pb2.Status,
    )
    results = proto.RepeatedField(
        proto.MESSAGE, number=2, message="MutateSmartCampaignSettingResult",
    )


class MutateSmartCampaignSettingResult(proto.Message):
    r"""The result for the Smart campaign setting mutate.

    Attributes:
        resource_name (str):
            Returned for successful operations.
        smart_campaign_setting (google.ads.googleads.v12.resources.types.SmartCampaignSetting):
            The mutated Smart campaign setting with only mutable fields
            after mutate. The field will only be returned when
            response_content_type is set to "MUTABLE_RESOURCE".
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    smart_campaign_setting = proto.Field(
        proto.MESSAGE,
        number=2,
        message=gagr_smart_campaign_setting.SmartCampaignSetting,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
