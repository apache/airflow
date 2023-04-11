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
    manifest={"FeedAttributeTypeEnum",},
)


class FeedAttributeTypeEnum(proto.Message):
    r"""Container for enum describing possible data types for a feed
    attribute.

    """

    class FeedAttributeType(proto.Enum):
        r"""Possible data types for a feed attribute."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        INT64 = 2
        DOUBLE = 3
        STRING = 4
        BOOLEAN = 5
        URL = 6
        DATE_TIME = 7
        INT64_LIST = 8
        DOUBLE_LIST = 9
        STRING_LIST = 10
        BOOLEAN_LIST = 11
        URL_LIST = 12
        DATE_TIME_LIST = 13
        PRICE = 14


__all__ = tuple(sorted(__protobuf__.manifest))
