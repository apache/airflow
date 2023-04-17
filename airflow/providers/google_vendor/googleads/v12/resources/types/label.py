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

from airflow.providers.google_vendor.googleads.v12.common.types import text_label as gagc_text_label
from airflow.providers.google_vendor.googleads.v12.enums.types import label_status


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"Label",},
)


class Label(proto.Message):
    r"""A label.

    Attributes:
        resource_name (str):
            Immutable. Name of the resource. Label resource names have
            the form: ``customers/{customer_id}/labels/{label_id}``
        id (int):
            Output only. ID of the label. Read only.

            This field is a member of `oneof`_ ``_id``.
        name (str):
            The name of the label.
            This field is required and should not be empty
            when creating a new label.
            The length of this string should be between 1
            and 80, inclusive.

            This field is a member of `oneof`_ ``_name``.
        status (google.ads.googleads.v12.enums.types.LabelStatusEnum.LabelStatus):
            Output only. Status of the label. Read only.
        text_label (google.ads.googleads.v12.common.types.TextLabel):
            A type of label displaying text on a colored
            background.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    id = proto.Field(proto.INT64, number=6, optional=True,)
    name = proto.Field(proto.STRING, number=7, optional=True,)
    status = proto.Field(
        proto.ENUM, number=4, enum=label_status.LabelStatusEnum.LabelStatus,
    )
    text_label = proto.Field(
        proto.MESSAGE, number=5, message=gagc_text_label.TextLabel,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
