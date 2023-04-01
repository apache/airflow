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
    package="google.ads.googleads.v12.enums",
    marshal="google.ads.googleads.v12",
    manifest={"LeadFormDesiredIntentEnum",},
)


class LeadFormDesiredIntentEnum(proto.Message):
    r"""Describes the chosen level of intent of generated leads.
    """

    class LeadFormDesiredIntent(proto.Enum):
        r"""Enum describing the chosen level of intent of generated
        leads.
        """
        UNSPECIFIED = 0
        UNKNOWN = 1
        LOW_INTENT = 2
        HIGH_INTENT = 3


__all__ = tuple(sorted(__protobuf__.manifest))
