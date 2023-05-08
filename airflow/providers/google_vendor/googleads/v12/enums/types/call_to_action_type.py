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
    manifest={"CallToActionTypeEnum",},
)


class CallToActionTypeEnum(proto.Message):
    r"""Container for enum describing the call to action types.
    """

    class CallToActionType(proto.Enum):
        r"""Enum describing possible types of call to action."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        LEARN_MORE = 2
        GET_QUOTE = 3
        APPLY_NOW = 4
        SIGN_UP = 5
        CONTACT_US = 6
        SUBSCRIBE = 7
        DOWNLOAD = 8
        BOOK_NOW = 9
        SHOP_NOW = 10


__all__ = tuple(sorted(__protobuf__.manifest))
