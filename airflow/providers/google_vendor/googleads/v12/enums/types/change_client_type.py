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
    manifest={"ChangeClientTypeEnum",},
)


class ChangeClientTypeEnum(proto.Message):
    r"""Container for enum describing the sources that the change
    event resource was made through.

    """

    class ChangeClientType(proto.Enum):
        r"""The source that the change_event resource was made through."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        GOOGLE_ADS_WEB_CLIENT = 2
        GOOGLE_ADS_AUTOMATED_RULE = 3
        GOOGLE_ADS_SCRIPTS = 4
        GOOGLE_ADS_BULK_UPLOAD = 5
        GOOGLE_ADS_API = 6
        GOOGLE_ADS_EDITOR = 7
        GOOGLE_ADS_MOBILE_APP = 8
        GOOGLE_ADS_RECOMMENDATIONS = 9
        SEARCH_ADS_360_SYNC = 10
        SEARCH_ADS_360_POST = 11
        INTERNAL_TOOL = 12
        OTHER = 13


__all__ = tuple(sorted(__protobuf__.manifest))
