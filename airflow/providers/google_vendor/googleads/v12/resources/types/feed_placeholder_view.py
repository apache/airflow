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

from airflow.providers.google_vendor.googleads.v12.enums.types import (
    placeholder_type as gage_placeholder_type,
)


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"FeedPlaceholderView",},
)


class FeedPlaceholderView(proto.Message):
    r"""A feed placeholder view.

    Attributes:
        resource_name (str):
            Output only. The resource name of the feed placeholder view.
            Feed placeholder view resource names have the form:

            ``customers/{customer_id}/feedPlaceholderViews/{placeholder_type}``
        placeholder_type (google.ads.googleads.v12.enums.types.PlaceholderTypeEnum.PlaceholderType):
            Output only. The placeholder type of the feed
            placeholder view.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    placeholder_type = proto.Field(
        proto.ENUM,
        number=2,
        enum=gage_placeholder_type.PlaceholderTypeEnum.PlaceholderType,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
