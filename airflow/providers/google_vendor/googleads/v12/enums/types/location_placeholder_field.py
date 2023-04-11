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
    manifest={"LocationPlaceholderFieldEnum",},
)


class LocationPlaceholderFieldEnum(proto.Message):
    r"""Values for Location placeholder fields.
    """

    class LocationPlaceholderField(proto.Enum):
        r"""Possible values for Location placeholder fields."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        BUSINESS_NAME = 2
        ADDRESS_LINE_1 = 3
        ADDRESS_LINE_2 = 4
        CITY = 5
        PROVINCE = 6
        POSTAL_CODE = 7
        COUNTRY_CODE = 8
        PHONE_NUMBER = 9


__all__ = tuple(sorted(__protobuf__.manifest))
