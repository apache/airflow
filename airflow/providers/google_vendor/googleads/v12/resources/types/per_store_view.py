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
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"PerStoreView",},
)


class PerStoreView(proto.Message):
    r"""An per store view.
    This view provides per store impression reach and local action
    conversion stats for advertisers.

    Attributes:
        resource_name (str):
            Output only. The resource name of the per store view. Per
            Store view resource names have the form:

            ``customers/{customer_id}/perStoreViews/{place_id}``
        place_id (str):
            Output only. The place ID of the per store
            view.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    place_id = proto.Field(proto.STRING, number=2,)


__all__ = tuple(sorted(__protobuf__.manifest))
