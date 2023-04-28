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


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.enums",
    marshal="google.ads.googleads.v12",
    manifest={"LocationExtensionTargetingCriterionFieldEnum",},
)


class LocationExtensionTargetingCriterionFieldEnum(proto.Message):
    r"""Values for Location Extension Targeting criterion fields.
    """

    class LocationExtensionTargetingCriterionField(proto.Enum):
        r"""Possible values for Location Extension Targeting criterion
        fields.
        """
        UNSPECIFIED = 0
        UNKNOWN = 1
        ADDRESS_LINE_1 = 2
        ADDRESS_LINE_2 = 3
        CITY = 4
        PROVINCE = 5
        POSTAL_CODE = 6
        COUNTRY_CODE = 7


__all__ = tuple(sorted(__protobuf__.manifest))
