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

from airflow.providers.google_vendor.googleads.v12.enums.types import merchant_center_link_status


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"MerchantCenterLink",},
)


class MerchantCenterLink(proto.Message):
    r"""A data sharing connection, proposed or in use,
    between a Google Ads Customer and a Merchant Center account.

    Attributes:
        resource_name (str):
            Immutable. The resource name of the merchant center link.
            Merchant center link resource names have the form:

            ``customers/{customer_id}/merchantCenterLinks/{merchant_center_id}``
        id (int):
            Output only. The ID of the Merchant Center
            account. This field is readonly.

            This field is a member of `oneof`_ ``_id``.
        merchant_center_account_name (str):
            Output only. The name of the Merchant Center
            account. This field is readonly.

            This field is a member of `oneof`_ ``_merchant_center_account_name``.
        status (google.ads.googleads.v12.enums.types.MerchantCenterLinkStatusEnum.MerchantCenterLinkStatus):
            The status of the link.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    id = proto.Field(proto.INT64, number=6, optional=True,)
    merchant_center_account_name = proto.Field(
        proto.STRING, number=7, optional=True,
    )
    status = proto.Field(
        proto.ENUM,
        number=5,
        enum=merchant_center_link_status.MerchantCenterLinkStatusEnum.MerchantCenterLinkStatus,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
