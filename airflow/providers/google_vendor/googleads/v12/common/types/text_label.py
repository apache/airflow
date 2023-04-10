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
    manifest={"TextLabel",},
)


class TextLabel(proto.Message):
    r"""A type of label displaying text on a colored background.

    Attributes:
        background_color (str):
            Background color of the label in RGB format. This string
            must match the regular expression
            '^#([a-fA-F0-9]{6}|[a-fA-F0-9]{3})$'. Note: The background
            color may not be visible for manager accounts.

            This field is a member of `oneof`_ ``_background_color``.
        description (str):
            A short description of the label. The length
            must be no more than 200 characters.

            This field is a member of `oneof`_ ``_description``.
    """

    background_color = proto.Field(proto.STRING, number=3, optional=True,)
    description = proto.Field(proto.STRING, number=4, optional=True,)


__all__ = tuple(sorted(__protobuf__.manifest))
