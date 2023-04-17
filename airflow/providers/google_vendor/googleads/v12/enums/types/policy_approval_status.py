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
    manifest={"PolicyApprovalStatusEnum",},
)


class PolicyApprovalStatusEnum(proto.Message):
    r"""Container for enum describing possible policy approval
    statuses.

    """

    class PolicyApprovalStatus(proto.Enum):
        r"""The possible policy approval statuses. When there are several
        approval statuses available the most severe one will be used. The
        order of severity is DISAPPROVED, AREA_OF_INTEREST_ONLY,
        APPROVED_LIMITED and APPROVED.
        """
        UNSPECIFIED = 0
        UNKNOWN = 1
        DISAPPROVED = 2
        APPROVED_LIMITED = 3
        APPROVED = 4
        AREA_OF_INTEREST_ONLY = 5


__all__ = tuple(sorted(__protobuf__.manifest))
