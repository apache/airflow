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
    package="google.ads.googleads.v12.common",
    marshal="google.ads.googleads.v12",
    manifest={"ExplorerAutoOptimizerSetting",},
)


class ExplorerAutoOptimizerSetting(proto.Message):
    r"""Settings for the Display Campaign Optimizer, initially named
    "Explorer". Learn more about `automatic
    targeting <https://support.google.com/google-ads/answer/190596>`__.

    Attributes:
        opt_in (bool):
            Indicates whether the optimizer is turned on.

            This field is a member of `oneof`_ ``_opt_in``.
    """

    opt_in = proto.Field(proto.BOOL, number=2, optional=True,)


__all__ = tuple(sorted(__protobuf__.manifest))
