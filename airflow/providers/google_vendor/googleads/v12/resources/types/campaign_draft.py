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

from airflow.providers.google_vendor.googleads.v12.enums.types import campaign_draft_status


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"CampaignDraft",},
)


class CampaignDraft(proto.Message):
    r"""A campaign draft.

    Attributes:
        resource_name (str):
            Immutable. The resource name of the campaign draft. Campaign
            draft resource names have the form:

            ``customers/{customer_id}/campaignDrafts/{base_campaign_id}~{draft_id}``
        draft_id (int):
            Output only. The ID of the draft.
            This field is read-only.

            This field is a member of `oneof`_ ``_draft_id``.
        base_campaign (str):
            Immutable. The base campaign to which the
            draft belongs.

            This field is a member of `oneof`_ ``_base_campaign``.
        name (str):
            The name of the campaign draft.
            This field is required and should not be empty
            when creating new campaign drafts.

            It must not contain any null (code point 0x0),
            NL line feed (code point 0xA) or carriage return
            (code point 0xD) characters.

            This field is a member of `oneof`_ ``_name``.
        draft_campaign (str):
            Output only. Resource name of the Campaign
            that results from overlaying the draft changes
            onto the base campaign.
            This field is read-only.

            This field is a member of `oneof`_ ``_draft_campaign``.
        status (google.ads.googleads.v12.enums.types.CampaignDraftStatusEnum.CampaignDraftStatus):
            Output only. The status of the campaign
            draft. This field is read-only.
            When a new campaign draft is added, the status
            defaults to PROPOSED.
        has_experiment_running (bool):
            Output only. Whether there is an experiment
            based on this draft currently serving.

            This field is a member of `oneof`_ ``_has_experiment_running``.
        long_running_operation (str):
            Output only. The resource name of the
            long-running operation that can be used to poll
            for completion of draft promotion. This is only
            set if the draft promotion is in progress or
            finished.

            This field is a member of `oneof`_ ``_long_running_operation``.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    draft_id = proto.Field(proto.INT64, number=9, optional=True,)
    base_campaign = proto.Field(proto.STRING, number=10, optional=True,)
    name = proto.Field(proto.STRING, number=11, optional=True,)
    draft_campaign = proto.Field(proto.STRING, number=12, optional=True,)
    status = proto.Field(
        proto.ENUM,
        number=6,
        enum=campaign_draft_status.CampaignDraftStatusEnum.CampaignDraftStatus,
    )
    has_experiment_running = proto.Field(proto.BOOL, number=13, optional=True,)
    long_running_operation = proto.Field(
        proto.STRING, number=14, optional=True,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
