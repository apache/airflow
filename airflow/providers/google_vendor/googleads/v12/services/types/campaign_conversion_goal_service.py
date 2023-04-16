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

from airflow.providers.google_vendor.googleads.v12.resources.types import campaign_conversion_goal
from google.protobuf import field_mask_pb2  # type: ignore


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.services",
    marshal="google.ads.googleads.v12",
    manifest={
        "MutateCampaignConversionGoalsRequest",
        "CampaignConversionGoalOperation",
        "MutateCampaignConversionGoalsResponse",
        "MutateCampaignConversionGoalResult",
    },
)


class MutateCampaignConversionGoalsRequest(proto.Message):
    r"""Request message for
    [CampaignConversionGoalService.MutateCampaignConversionGoals][google.ads.googleads.v12.services.CampaignConversionGoalService.MutateCampaignConversionGoals].

    Attributes:
        customer_id (str):
            Required. The ID of the customer whose
            campaign conversion goals are being modified.
        operations (Sequence[google.ads.googleads.v12.services.types.CampaignConversionGoalOperation]):
            Required. The list of operations to perform
            on individual campaign conversion goal.
        validate_only (bool):
            If true, the request is validated but not
            executed. Only errors are returned, not results.
    """

    customer_id = proto.Field(proto.STRING, number=1,)
    operations = proto.RepeatedField(
        proto.MESSAGE, number=2, message="CampaignConversionGoalOperation",
    )
    validate_only = proto.Field(proto.BOOL, number=3,)


class CampaignConversionGoalOperation(proto.Message):
    r"""A single operation (update) on a campaign conversion goal.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        update_mask (google.protobuf.field_mask_pb2.FieldMask):
            FieldMask that determines which resource
            fields are modified in an update.
        update (google.ads.googleads.v12.resources.types.CampaignConversionGoal):
            Update operation: The customer conversion
            goal is expected to have a valid resource name.

            This field is a member of `oneof`_ ``operation``.
    """

    update_mask = proto.Field(
        proto.MESSAGE, number=2, message=field_mask_pb2.FieldMask,
    )
    update = proto.Field(
        proto.MESSAGE,
        number=1,
        oneof="operation",
        message=campaign_conversion_goal.CampaignConversionGoal,
    )


class MutateCampaignConversionGoalsResponse(proto.Message):
    r"""Response message for a campaign conversion goal mutate.

    Attributes:
        results (Sequence[google.ads.googleads.v12.services.types.MutateCampaignConversionGoalResult]):
            All results for the mutate.
    """

    results = proto.RepeatedField(
        proto.MESSAGE, number=1, message="MutateCampaignConversionGoalResult",
    )


class MutateCampaignConversionGoalResult(proto.Message):
    r"""The result for the campaign conversion goal mutate.

    Attributes:
        resource_name (str):
            Returned for successful operations.
    """

    resource_name = proto.Field(proto.STRING, number=1,)


__all__ = tuple(sorted(__protobuf__.manifest))
