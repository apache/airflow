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
    manifest={"ProductChannelExclusivityEnum",},
)


class ProductChannelExclusivityEnum(proto.Message):
    r"""Availability of a product offer.
    """

    class ProductChannelExclusivity(proto.Enum):
        r"""Enum describing the availability of a product offer."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        SINGLE_CHANNEL = 2
        MULTI_CHANNEL = 3


__all__ = tuple(sorted(__protobuf__.manifest))
