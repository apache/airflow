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

from airflow.providers.google_vendor.googleads.v12.enums.types import extension_setting_device
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    extension_type as gage_extension_type,
)


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"CustomerExtensionSetting",},
)


class CustomerExtensionSetting(proto.Message):
    r"""A customer extension setting.

    Attributes:
        resource_name (str):
            Immutable. The resource name of the customer extension
            setting. CustomerExtensionSetting resource names have the
            form:

            ``customers/{customer_id}/customerExtensionSettings/{extension_type}``
        extension_type (google.ads.googleads.v12.enums.types.ExtensionTypeEnum.ExtensionType):
            Immutable. The extension type of the customer
            extension setting.
        extension_feed_items (Sequence[str]):
            The resource names of the extension feed items to serve
            under the customer. ExtensionFeedItem resource names have
            the form:

            ``customers/{customer_id}/extensionFeedItems/{feed_item_id}``
        device (google.ads.googleads.v12.enums.types.ExtensionSettingDeviceEnum.ExtensionSettingDevice):
            The device for which the extensions will
            serve. Optional.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    extension_type = proto.Field(
        proto.ENUM,
        number=2,
        enum=gage_extension_type.ExtensionTypeEnum.ExtensionType,
    )
    extension_feed_items = proto.RepeatedField(proto.STRING, number=5,)
    device = proto.Field(
        proto.ENUM,
        number=4,
        enum=extension_setting_device.ExtensionSettingDeviceEnum.ExtensionSettingDevice,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
