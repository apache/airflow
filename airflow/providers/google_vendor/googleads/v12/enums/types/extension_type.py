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
    manifest={"ExtensionTypeEnum",},
)


class ExtensionTypeEnum(proto.Message):
    r"""Container for enum describing possible data types for an
    extension in an extension setting.

    """

    class ExtensionType(proto.Enum):
        r"""Possible data types for an extension in an extension setting."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        NONE = 2
        APP = 3
        CALL = 4
        CALLOUT = 5
        MESSAGE = 6
        PRICE = 7
        PROMOTION = 8
        SITELINK = 10
        STRUCTURED_SNIPPET = 11
        LOCATION = 12
        AFFILIATE_LOCATION = 13
        HOTEL_CALLOUT = 15
        IMAGE = 16


__all__ = tuple(sorted(__protobuf__.manifest))
