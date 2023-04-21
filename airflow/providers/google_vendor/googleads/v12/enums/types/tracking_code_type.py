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
    manifest={"TrackingCodeTypeEnum",},
)


class TrackingCodeTypeEnum(proto.Message):
    r"""Container for enum describing the type of the generated tag
    snippets for tracking conversions.

    """

    class TrackingCodeType(proto.Enum):
        r"""The type of the generated tag snippets for tracking
        conversions.
        """
        UNSPECIFIED = 0
        UNKNOWN = 1
        WEBPAGE = 2
        WEBPAGE_ONCLICK = 3
        CLICK_TO_CALL = 4
        WEBSITE_CALL = 5


__all__ = tuple(sorted(__protobuf__.manifest))
