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
    manifest={"UserListSizeRangeEnum",},
)


class UserListSizeRangeEnum(proto.Message):
    r"""Size range in terms of number of users of a UserList.
    """

    class UserListSizeRange(proto.Enum):
        r"""Enum containing possible user list size ranges."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        LESS_THAN_FIVE_HUNDRED = 2
        LESS_THAN_ONE_THOUSAND = 3
        ONE_THOUSAND_TO_TEN_THOUSAND = 4
        TEN_THOUSAND_TO_FIFTY_THOUSAND = 5
        FIFTY_THOUSAND_TO_ONE_HUNDRED_THOUSAND = 6
        ONE_HUNDRED_THOUSAND_TO_THREE_HUNDRED_THOUSAND = 7
        THREE_HUNDRED_THOUSAND_TO_FIVE_HUNDRED_THOUSAND = 8
        FIVE_HUNDRED_THOUSAND_TO_ONE_MILLION = 9
        ONE_MILLION_TO_TWO_MILLION = 10
        TWO_MILLION_TO_THREE_MILLION = 11
        THREE_MILLION_TO_FIVE_MILLION = 12
        FIVE_MILLION_TO_TEN_MILLION = 13
        TEN_MILLION_TO_TWENTY_MILLION = 14
        TWENTY_MILLION_TO_THIRTY_MILLION = 15
        THIRTY_MILLION_TO_FIFTY_MILLION = 16
        OVER_FIFTY_MILLION = 17


__all__ = tuple(sorted(__protobuf__.manifest))
