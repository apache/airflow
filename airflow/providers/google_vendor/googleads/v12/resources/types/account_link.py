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

from airflow.providers.google_vendor.googleads.v12.enums.types import account_link_status
from airflow.providers.google_vendor.googleads.v12.enums.types import linked_account_type
from airflow.providers.google_vendor.googleads.v12.enums.types import mobile_app_vendor


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={
        "AccountLink",
        "ThirdPartyAppAnalyticsLinkIdentifier",
        "DataPartnerLinkIdentifier",
        "HotelCenterLinkIdentifier",
        "GoogleAdsLinkIdentifier",
        "AdvertisingPartnerLinkIdentifier",
    },
)


class AccountLink(proto.Message):
    r"""Represents the data sharing connection between a Google Ads
    account and another account

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        resource_name (str):
            Immutable. Resource name of the account link. AccountLink
            resource names have the form:
            ``customers/{customer_id}/accountLinks/{account_link_id}``
        account_link_id (int):
            Output only. The ID of the link.
            This field is read only.

            This field is a member of `oneof`_ ``_account_link_id``.
        status (google.ads.googleads.v12.enums.types.AccountLinkStatusEnum.AccountLinkStatus):
            The status of the link.
        type_ (google.ads.googleads.v12.enums.types.LinkedAccountTypeEnum.LinkedAccountType):
            Output only. The type of the linked account.
        third_party_app_analytics (google.ads.googleads.v12.resources.types.ThirdPartyAppAnalyticsLinkIdentifier):
            Immutable. A third party app analytics link.

            This field is a member of `oneof`_ ``linked_account``.
        data_partner (google.ads.googleads.v12.resources.types.DataPartnerLinkIdentifier):
            Output only. Data partner link.

            This field is a member of `oneof`_ ``linked_account``.
        google_ads (google.ads.googleads.v12.resources.types.GoogleAdsLinkIdentifier):
            Output only. Google Ads link.

            This field is a member of `oneof`_ ``linked_account``.
        hotel_center (google.ads.googleads.v12.resources.types.HotelCenterLinkIdentifier):
            Output only. Hotel link

            This field is a member of `oneof`_ ``linked_account``.
        advertising_partner (google.ads.googleads.v12.resources.types.AdvertisingPartnerLinkIdentifier):
            Output only. Advertising Partner link

            This field is a member of `oneof`_ ``linked_account``.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    account_link_id = proto.Field(proto.INT64, number=8, optional=True,)
    status = proto.Field(
        proto.ENUM,
        number=3,
        enum=account_link_status.AccountLinkStatusEnum.AccountLinkStatus,
    )
    type_ = proto.Field(
        proto.ENUM,
        number=4,
        enum=linked_account_type.LinkedAccountTypeEnum.LinkedAccountType,
    )
    third_party_app_analytics = proto.Field(
        proto.MESSAGE,
        number=5,
        oneof="linked_account",
        message="ThirdPartyAppAnalyticsLinkIdentifier",
    )
    data_partner = proto.Field(
        proto.MESSAGE,
        number=6,
        oneof="linked_account",
        message="DataPartnerLinkIdentifier",
    )
    google_ads = proto.Field(
        proto.MESSAGE,
        number=7,
        oneof="linked_account",
        message="GoogleAdsLinkIdentifier",
    )
    hotel_center = proto.Field(
        proto.MESSAGE,
        number=9,
        oneof="linked_account",
        message="HotelCenterLinkIdentifier",
    )
    advertising_partner = proto.Field(
        proto.MESSAGE,
        number=10,
        oneof="linked_account",
        message="AdvertisingPartnerLinkIdentifier",
    )


class ThirdPartyAppAnalyticsLinkIdentifier(proto.Message):
    r"""The identifiers of a Third Party App Analytics Link.

    Attributes:
        app_analytics_provider_id (int):
            Immutable. The ID of the app analytics
            provider. This field should not be empty when
            creating a new third party app analytics link.
            It is unable to be modified after the creation
            of the link.

            This field is a member of `oneof`_ ``_app_analytics_provider_id``.
        app_id (str):
            Immutable. A string that uniquely identifies
            a mobile application from which the data was
            collected to the Google Ads API. For iOS, the ID
            string is the 9 digit string that appears at the
            end of an App Store URL (for example,
            "422689480" for "Gmail" whose App Store link is
            https://apps.apple.com/us/app/gmail-email-by-google/id422689480).
            For Android, the ID string is the application's
            package name (for example,
            "com.google.android.gm" for "Gmail" given Google
            Play link
            https://play.google.com/store/apps/details?id=com.google.android.gm)
            This field should not be empty when creating a
            new third party app analytics link. It is unable
            to be modified after the creation of the link.

            This field is a member of `oneof`_ ``_app_id``.
        app_vendor (google.ads.googleads.v12.enums.types.MobileAppVendorEnum.MobileAppVendor):
            Immutable. The vendor of the app.
            This field should not be empty when creating a
            new third party app analytics link. It is unable
            to be modified after the creation of the link.
    """

    app_analytics_provider_id = proto.Field(
        proto.INT64, number=4, optional=True,
    )
    app_id = proto.Field(proto.STRING, number=5, optional=True,)
    app_vendor = proto.Field(
        proto.ENUM,
        number=3,
        enum=mobile_app_vendor.MobileAppVendorEnum.MobileAppVendor,
    )


class DataPartnerLinkIdentifier(proto.Message):
    r"""The identifier for Data Partner account.

    Attributes:
        data_partner_id (int):
            Immutable. The customer ID of the Data
            partner account. This field is required and
            should not be empty when creating a new data
            partner link. It is unable to be modified after
            the creation of the link.

            This field is a member of `oneof`_ ``_data_partner_id``.
    """

    data_partner_id = proto.Field(proto.INT64, number=1, optional=True,)


class HotelCenterLinkIdentifier(proto.Message):
    r"""The identifier for Hotel account.

    Attributes:
        hotel_center_id (int):
            Output only. The hotel center id of the hotel
            account.
    """

    hotel_center_id = proto.Field(proto.INT64, number=1,)


class GoogleAdsLinkIdentifier(proto.Message):
    r"""The identifier for Google Ads account.

    Attributes:
        customer (str):
            Immutable. The resource name of the Google
            Ads account. This field is required and should
            not be empty when creating a new Google Ads
            link. It is unable to be modified after the
            creation of the link.

            This field is a member of `oneof`_ ``_customer``.
    """

    customer = proto.Field(proto.STRING, number=3, optional=True,)


class AdvertisingPartnerLinkIdentifier(proto.Message):
    r"""The identifier for the Advertising Partner Google Ads
    account.

    Attributes:
        customer (str):
            Immutable. The resource name of the
            advertising partner Google Ads account. This
            field is required and should not be empty when
            creating a new Advertising Partner link. It is
            unable to be modified after the creation of the
            link.

            This field is a member of `oneof`_ ``_customer``.
    """

    customer = proto.Field(proto.STRING, number=1, optional=True,)


__all__ = tuple(sorted(__protobuf__.manifest))
