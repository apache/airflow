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
    manifest={"LinkedAccountTypeEnum",},
)


class LinkedAccountTypeEnum(proto.Message):
    r"""Container for enum describing different types of Linked
    accounts.

    """

    class LinkedAccountType(proto.Enum):
        r"""Describes the possible link types between a Google Ads
        customer and another account.
        """
        UNSPECIFIED = 0
        UNKNOWN = 1
        THIRD_PARTY_APP_ANALYTICS = 2
        DATA_PARTNER = 3
        GOOGLE_ADS = 4
        HOTEL_CENTER = 5
        ADVERTISING_PARTNER = 6


__all__ = tuple(sorted(__protobuf__.manifest))
