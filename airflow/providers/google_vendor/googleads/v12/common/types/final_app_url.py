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

from airflow.providers.google_vendor.googleads.v12.enums.types import app_url_operating_system_type


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.common",
    marshal="google.ads.googleads.v12",
    manifest={"FinalAppUrl",},
)


class FinalAppUrl(proto.Message):
    r"""A URL for deep linking into an app for the given operating
    system.

    Attributes:
        os_type (google.ads.googleads.v12.enums.types.AppUrlOperatingSystemTypeEnum.AppUrlOperatingSystemType):
            The operating system targeted by this URL.
            Required.
        url (str):
            The app deep link URL. Deep links specify a location in an
            app that corresponds to the content you'd like to show, and
            should be of the form {scheme}://{host_path} The scheme
            identifies which app to open. For your app, you can use a
            custom scheme that starts with the app's name. The host and
            path specify the unique location in the app where your
            content exists. Example: "exampleapp://productid_1234".
            Required.

            This field is a member of `oneof`_ ``_url``.
    """

    os_type = proto.Field(
        proto.ENUM,
        number=1,
        enum=app_url_operating_system_type.AppUrlOperatingSystemTypeEnum.AppUrlOperatingSystemType,
    )
    url = proto.Field(proto.STRING, number=3, optional=True,)


__all__ = tuple(sorted(__protobuf__.manifest))
