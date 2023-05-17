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
    manifest={"UrlCollection",},
)


class UrlCollection(proto.Message):
    r"""Collection of urls that is tagged with a unique identifier.

    Attributes:
        url_collection_id (str):
            Unique identifier for this UrlCollection
            instance.

            This field is a member of `oneof`_ ``_url_collection_id``.
        final_urls (Sequence[str]):
            A list of possible final URLs.
        final_mobile_urls (Sequence[str]):
            A list of possible final mobile URLs.
        tracking_url_template (str):
            URL template for constructing a tracking URL.

            This field is a member of `oneof`_ ``_tracking_url_template``.
    """

    url_collection_id = proto.Field(proto.STRING, number=5, optional=True,)
    final_urls = proto.RepeatedField(proto.STRING, number=6,)
    final_mobile_urls = proto.RepeatedField(proto.STRING, number=7,)
    tracking_url_template = proto.Field(proto.STRING, number=8, optional=True,)


__all__ = tuple(sorted(__protobuf__.manifest))
