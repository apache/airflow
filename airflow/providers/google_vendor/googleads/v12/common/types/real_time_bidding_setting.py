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
    package="airflow.providers.google_vendor.googleads.v12.common",
    marshal="google.ads.googleads.v12",
    manifest={"RealTimeBiddingSetting",},
)


class RealTimeBiddingSetting(proto.Message):
    r"""Settings for Real-Time Bidding, a feature only available for
    campaigns targeting the Ad Exchange network.

    Attributes:
        opt_in (bool):
            Whether the campaign is opted in to real-time
            bidding.

            This field is a member of `oneof`_ ``_opt_in``.
    """

    opt_in = proto.Field(proto.BOOL, number=2, optional=True,)


__all__ = tuple(sorted(__protobuf__.manifest))
