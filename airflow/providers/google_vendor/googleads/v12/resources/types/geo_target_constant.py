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

from airflow.providers.google_vendor.googleads.v12.enums.types import geo_target_constant_status


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"GeoTargetConstant",},
)


class GeoTargetConstant(proto.Message):
    r"""A geo target constant.

    Attributes:
        resource_name (str):
            Output only. The resource name of the geo target constant.
            Geo target constant resource names have the form:

            ``geoTargetConstants/{geo_target_constant_id}``
        id (int):
            Output only. The ID of the geo target
            constant.

            This field is a member of `oneof`_ ``_id``.
        name (str):
            Output only. Geo target constant English
            name.

            This field is a member of `oneof`_ ``_name``.
        country_code (str):
            Output only. The ISO-3166-1 alpha-2 country
            code that is associated with the target.

            This field is a member of `oneof`_ ``_country_code``.
        target_type (str):
            Output only. Geo target constant target type.

            This field is a member of `oneof`_ ``_target_type``.
        status (google.ads.googleads.v12.enums.types.GeoTargetConstantStatusEnum.GeoTargetConstantStatus):
            Output only. Geo target constant status.
        canonical_name (str):
            Output only. The fully qualified English
            name, consisting of the target's name and that
            of its parent and country.

            This field is a member of `oneof`_ ``_canonical_name``.
        parent_geo_target (str):
            Output only. The resource name of the parent geo target
            constant. Geo target constant resource names have the form:

            ``geoTargetConstants/{parent_geo_target_constant_id}``

            This field is a member of `oneof`_ ``_parent_geo_target``.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    id = proto.Field(proto.INT64, number=10, optional=True,)
    name = proto.Field(proto.STRING, number=11, optional=True,)
    country_code = proto.Field(proto.STRING, number=12, optional=True,)
    target_type = proto.Field(proto.STRING, number=13, optional=True,)
    status = proto.Field(
        proto.ENUM,
        number=7,
        enum=geo_target_constant_status.GeoTargetConstantStatusEnum.GeoTargetConstantStatus,
    )
    canonical_name = proto.Field(proto.STRING, number=14, optional=True,)
    parent_geo_target = proto.Field(proto.STRING, number=9, optional=True,)


__all__ = tuple(sorted(__protobuf__.manifest))
