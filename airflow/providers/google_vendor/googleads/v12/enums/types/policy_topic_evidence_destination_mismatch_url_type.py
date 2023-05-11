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
    manifest={"PolicyTopicEvidenceDestinationMismatchUrlTypeEnum",},
)


class PolicyTopicEvidenceDestinationMismatchUrlTypeEnum(proto.Message):
    r"""Container for enum describing possible policy topic evidence
    destination mismatch url types.

    """

    class PolicyTopicEvidenceDestinationMismatchUrlType(proto.Enum):
        r"""The possible policy topic evidence destination mismatch url
        types.
        """
        UNSPECIFIED = 0
        UNKNOWN = 1
        DISPLAY_URL = 2
        FINAL_URL = 3
        FINAL_MOBILE_URL = 4
        TRACKING_URL = 5
        MOBILE_TRACKING_URL = 6


__all__ = tuple(sorted(__protobuf__.manifest))
