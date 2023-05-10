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
    manifest={"LeadFormCallToActionTypeEnum",},
)


class LeadFormCallToActionTypeEnum(proto.Message):
    r"""Describes the type of call-to-action phrases in a lead form.
    """

    class LeadFormCallToActionType(proto.Enum):
        r"""Enum describing the type of call-to-action phrases in a lead
        form.
        """
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
        GET_OFFER = 10
        REGISTER = 11
        GET_INFO = 12
        REQUEST_DEMO = 13
        JOIN_NOW = 14
        GET_STARTED = 15


__all__ = tuple(sorted(__protobuf__.manifest))
