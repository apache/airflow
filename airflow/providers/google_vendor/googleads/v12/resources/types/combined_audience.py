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

from airflow.providers.google_vendor.googleads.v12.enums.types import combined_audience_status


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"CombinedAudience",},
)


class CombinedAudience(proto.Message):
    r"""Describe a resource for combined audiences which includes
    different audiences.

    Attributes:
        resource_name (str):
            Immutable. The resource name of the combined audience.
            Combined audience names have the form:

            ``customers/{customer_id}/combinedAudience/{combined_audience_id}``
        id (int):
            Output only. ID of the combined audience.
        status (google.ads.googleads.v12.enums.types.CombinedAudienceStatusEnum.CombinedAudienceStatus):
            Output only. Status of this combined
            audience. Indicates whether the combined
            audience is enabled or removed.
        name (str):
            Output only. Name of the combined audience.
            It should be unique across all combined
            audiences.
        description (str):
            Output only. Description of this combined
            audience.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    id = proto.Field(proto.INT64, number=2,)
    status = proto.Field(
        proto.ENUM,
        number=3,
        enum=combined_audience_status.CombinedAudienceStatusEnum.CombinedAudienceStatus,
    )
    name = proto.Field(proto.STRING, number=4,)
    description = proto.Field(proto.STRING, number=5,)


__all__ = tuple(sorted(__protobuf__.manifest))
