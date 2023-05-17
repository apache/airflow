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

from airflow.providers.google_vendor.googleads.v12.common.types import audiences
from airflow.providers.google_vendor.googleads.v12.enums.types import audience_status


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"Audience",},
)


class Audience(proto.Message):
    r"""Audience is an effective targeting option that lets you
    intersect different segment attributes, such as detailed
    demographics and affinities, to create audiences that represent
    sections of your target segments.

    Attributes:
        resource_name (str):
            Immutable. The resource name of the audience. Audience names
            have the form:

            ``customers/{customer_id}/audiences/{audience_id}``
        id (int):
            Output only. ID of the audience.
        status (google.ads.googleads.v12.enums.types.AudienceStatusEnum.AudienceStatus):
            Output only. Status of this audience.
            Indicates whether the audience is enabled or
            removed.
        name (str):
            Required. Name of the audience. It should be
            unique across all audiences. It must have a
            minimum length of 1 and maximum length of 255.
        description (str):
            Description of this audience.
        dimensions (Sequence[google.ads.googleads.v12.common.types.AudienceDimension]):
            Positive dimensions specifying the audience
            composition.
        exclusion_dimension (google.ads.googleads.v12.common.types.AudienceExclusionDimension):
            Negative dimension specifying the audience
            composition.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    id = proto.Field(proto.INT64, number=2,)
    status = proto.Field(
        proto.ENUM,
        number=3,
        enum=audience_status.AudienceStatusEnum.AudienceStatus,
    )
    name = proto.Field(proto.STRING, number=4,)
    description = proto.Field(proto.STRING, number=5,)
    dimensions = proto.RepeatedField(
        proto.MESSAGE, number=6, message=audiences.AudienceDimension,
    )
    exclusion_dimension = proto.Field(
        proto.MESSAGE, number=7, message=audiences.AudienceExclusionDimension,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
