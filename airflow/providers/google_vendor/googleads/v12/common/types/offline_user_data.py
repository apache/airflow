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
    user_identifier_source as gage_user_identifier_source,
)


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.common",
    marshal="google.ads.googleads.v12",
    manifest={
        "OfflineUserAddressInfo",
        "UserIdentifier",
        "TransactionAttribute",
        "StoreAttribute",
        "ItemAttribute",
        "UserData",
        "UserAttribute",
        "EventAttribute",
        "EventItemAttribute",
        "ShoppingLoyalty",
        "CustomerMatchUserListMetadata",
        "StoreSalesMetadata",
        "StoreSalesThirdPartyMetadata",
    },
)


class OfflineUserAddressInfo(proto.Message):
    r"""Address identifier of offline data.

    Attributes:
        hashed_first_name (str):
            First name of the user, which is hashed as
            SHA-256 after normalized (Lowercase all
            characters; Remove any extra spaces before,
            after, and in between).

            This field is a member of `oneof`_ ``_hashed_first_name``.
        hashed_last_name (str):
            Last name of the user, which is hashed as
            SHA-256 after normalized (lower case only and no
            punctuation).

            This field is a member of `oneof`_ ``_hashed_last_name``.
        city (str):
            City of the address. Only accepted for Store
            Sales and ConversionAdjustmentUploadService.

            This field is a member of `oneof`_ ``_city``.
        state (str):
            State code of the address. Only accepted for
            Store Sales and
            ConversionAdjustmentUploadService.

            This field is a member of `oneof`_ ``_state``.
        country_code (str):
            2-letter country code in ISO-3166-1 alpha-2
            of the user's address.

            This field is a member of `oneof`_ ``_country_code``.
        postal_code (str):
            Postal code of the user's address.

            This field is a member of `oneof`_ ``_postal_code``.
        hashed_street_address (str):
            The street address of the user hashed using
            SHA-256 hash function after normalization (lower
            case only). Only accepted for
            ConversionAdjustmentUploadService.

            This field is a member of `oneof`_ ``_hashed_street_address``.
    """

    hashed_first_name = proto.Field(proto.STRING, number=7, optional=True,)
    hashed_last_name = proto.Field(proto.STRING, number=8, optional=True,)
    city = proto.Field(proto.STRING, number=9, optional=True,)
    state = proto.Field(proto.STRING, number=10, optional=True,)
    country_code = proto.Field(proto.STRING, number=11, optional=True,)
    postal_code = proto.Field(proto.STRING, number=12, optional=True,)
    hashed_street_address = proto.Field(proto.STRING, number=13, optional=True,)


class UserIdentifier(proto.Message):
    r"""User identifying information.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        user_identifier_source (google.ads.googleads.v12.enums.types.UserIdentifierSourceEnum.UserIdentifierSource):
            Source of the user identifier when the upload
            is from Store Sales, ConversionUploadService, or
            ConversionAdjustmentUploadService.
        hashed_email (str):
            Hashed email address using SHA-256 hash
            function after normalization. Accepted for
            Customer Match, Store Sales,
            ConversionUploadService, and
            ConversionAdjustmentUploadService.

            This field is a member of `oneof`_ ``identifier``.
        hashed_phone_number (str):
            Hashed phone number using SHA-256 hash
            function after normalization (E164 standard).
            Accepted for Customer Match, Store Sales,
            ConversionUploadService, and
            ConversionAdjustmentUploadService.

            This field is a member of `oneof`_ ``identifier``.
        mobile_id (str):
            Mobile device ID (advertising ID/IDFA).
            Accepted only for Customer Match.

            This field is a member of `oneof`_ ``identifier``.
        third_party_user_id (str):
            Advertiser-assigned user ID for Customer
            Match upload, or third-party-assigned user ID
            for Store Sales. Accepted only for Customer
            Match and Store Sales.

            This field is a member of `oneof`_ ``identifier``.
        address_info (google.ads.googleads.v12.common.types.OfflineUserAddressInfo):
            Address information. Accepted only for
            Customer Match, Store Sales, and
            ConversionAdjustmentUploadService.

            This field is a member of `oneof`_ ``identifier``.
    """

    user_identifier_source = proto.Field(
        proto.ENUM,
        number=6,
        enum=gage_user_identifier_source.UserIdentifierSourceEnum.UserIdentifierSource,
    )
    hashed_email = proto.Field(proto.STRING, number=7, oneof="identifier",)
    hashed_phone_number = proto.Field(
        proto.STRING, number=8, oneof="identifier",
    )
    mobile_id = proto.Field(proto.STRING, number=9, oneof="identifier",)
    third_party_user_id = proto.Field(
        proto.STRING, number=10, oneof="identifier",
    )
    address_info = proto.Field(
        proto.MESSAGE,
        number=5,
        oneof="identifier",
        message="OfflineUserAddressInfo",
    )


class TransactionAttribute(proto.Message):
    r"""Attribute of the store sales transaction.

    Attributes:
        transaction_date_time (str):
            Timestamp when transaction occurred. Required. The format is
            "YYYY-MM-DD HH:MM:SS[+/-HH:MM]", where [+/-HH:MM] is an
            optional timezone offset from UTC. If the offset is absent,
            the API will use the account's timezone as default.
            Examples: "2018-03-05 09:15:00" or "2018-02-01
            14:34:30+03:00".

            This field is a member of `oneof`_ ``_transaction_date_time``.
        transaction_amount_micros (float):
            Transaction amount in micros. Required.
            Transaction amount in micros needs to be greater
            than 1000. If item Attributes are provided, it
            represents the total value of the items, after
            multiplying the unit price per item by the
            quantity provided in the ItemAttributes.

            This field is a member of `oneof`_ ``_transaction_amount_micros``.
        currency_code (str):
            Transaction currency code. ISO 4217
            three-letter code is used. Required.

            This field is a member of `oneof`_ ``_currency_code``.
        conversion_action (str):
            The resource name of conversion action to
            report conversions to. Required.

            This field is a member of `oneof`_ ``_conversion_action``.
        order_id (str):
            Transaction order id.
            Accessible only to customers on the allow-list.

            This field is a member of `oneof`_ ``_order_id``.
        store_attribute (google.ads.googleads.v12.common.types.StoreAttribute):
            Store attributes of the transaction.
            Accessible only to customers on the allow-list.
        custom_value (str):
            Value of the custom variable for each
            transaction. Accessible only to customers on the
            allow-list.

            This field is a member of `oneof`_ ``_custom_value``.
        item_attribute (google.ads.googleads.v12.common.types.ItemAttribute):
            Item attributes of the transaction.
    """

    transaction_date_time = proto.Field(proto.STRING, number=8, optional=True,)
    transaction_amount_micros = proto.Field(
        proto.DOUBLE, number=9, optional=True,
    )
    currency_code = proto.Field(proto.STRING, number=10, optional=True,)
    conversion_action = proto.Field(proto.STRING, number=11, optional=True,)
    order_id = proto.Field(proto.STRING, number=12, optional=True,)
    store_attribute = proto.Field(
        proto.MESSAGE, number=6, message="StoreAttribute",
    )
    custom_value = proto.Field(proto.STRING, number=13, optional=True,)
    item_attribute = proto.Field(
        proto.MESSAGE, number=14, message="ItemAttribute",
    )


class StoreAttribute(proto.Message):
    r"""Store attributes of the transaction.

    Attributes:
        store_code (str):
            Store code from
            https://support.google.com/business/answer/3370250#storecode

            This field is a member of `oneof`_ ``_store_code``.
    """

    store_code = proto.Field(proto.STRING, number=2, optional=True,)


class ItemAttribute(proto.Message):
    r"""Item attributes of the transaction.

    Attributes:
        item_id (str):
            A unique identifier of a product. It can be
            either the Merchant Center Item ID or GTIN
            (Global Trade Item Number).
        merchant_id (int):
            ID of the Merchant Center Account.

            This field is a member of `oneof`_ ``_merchant_id``.
        country_code (str):
            Common Locale Data Repository (CLDR)
            territory code of the country associated with
            the feed where your items are uploaded. See
            https://developers.google.com/google-ads/api/reference/data/codes-formats#country-codes
            for more information.
        language_code (str):
            ISO 639-1 code of the language associated
            with the feed where your items are uploaded
        quantity (int):
            The number of items sold. Defaults to 1 if
            not set.
    """

    item_id = proto.Field(proto.STRING, number=1,)
    merchant_id = proto.Field(proto.INT64, number=2, optional=True,)
    country_code = proto.Field(proto.STRING, number=3,)
    language_code = proto.Field(proto.STRING, number=4,)
    quantity = proto.Field(proto.INT64, number=5,)


class UserData(proto.Message):
    r"""User data holding user identifiers and attributes.

    Attributes:
        user_identifiers (Sequence[google.ads.googleads.v12.common.types.UserIdentifier]):
            User identification info. Required.
        transaction_attribute (google.ads.googleads.v12.common.types.TransactionAttribute):
            Additional transactions/attributes associated
            with the user. Required when updating store
            sales data.
        user_attribute (google.ads.googleads.v12.common.types.UserAttribute):
            Additional attributes associated with the
            user. Required when updating customer match
            attributes. These have an expiration of 540
            days.
    """

    user_identifiers = proto.RepeatedField(
        proto.MESSAGE, number=1, message="UserIdentifier",
    )
    transaction_attribute = proto.Field(
        proto.MESSAGE, number=2, message="TransactionAttribute",
    )
    user_attribute = proto.Field(
        proto.MESSAGE, number=3, message="UserAttribute",
    )


class UserAttribute(proto.Message):
    r"""User attribute, can only be used with CUSTOMER_MATCH_WITH_ATTRIBUTES
    job type.

    Attributes:
        lifetime_value_micros (int):
            Advertiser defined lifetime value for the
            user.

            This field is a member of `oneof`_ ``_lifetime_value_micros``.
        lifetime_value_bucket (int):
            Advertiser defined lifetime value bucket for
            the user. The valid range for a lifetime value
            bucket is from 1 (low) to 10 (high), except for
            remove operation where 0 will also be accepted.

            This field is a member of `oneof`_ ``_lifetime_value_bucket``.
        last_purchase_date_time (str):
            Timestamp of the last purchase made by the user. The format
            is YYYY-MM-DD HH:MM:SS[+/-HH:MM], where [+/-HH:MM] is an
            optional timezone offset from UTC. If the offset is absent,
            the API will use the account's timezone as default.
        average_purchase_count (int):
            Advertiser defined average number of
            purchases that are made by the user in a 30 day
            period.
        average_purchase_value_micros (int):
            Advertiser defined average purchase value in
            micros for the user.
        acquisition_date_time (str):
            Timestamp when the user was acquired. The format is
            YYYY-MM-DD HH:MM:SS[+/-HH:MM], where [+/-HH:MM] is an
            optional timezone offset from UTC. If the offset is absent,
            the API will use the account's timezone as default.
        shopping_loyalty (google.ads.googleads.v12.common.types.ShoppingLoyalty):
            The shopping loyalty related data. Shopping
            utilizes this data to provide users with a
            better experience. Accessible only to merchants
            on the allow-list with the user's consent.

            This field is a member of `oneof`_ ``_shopping_loyalty``.
        lifecycle_stage (str):
            Optional. Advertiser defined lifecycle stage
            for the user. The accepted values are "Lead",
            "Active" and "Churned".
        first_purchase_date_time (str):
            Optional. Timestamp of the first purchase made by the user.
            The format is YYYY-MM-DD HH:MM:SS[+/-HH:MM], where
            [+/-HH:MM] is an optional timezone offset from UTC. If the
            offset is absent, the API will use the account's timezone as
            default.
        event_attribute (Sequence[google.ads.googleads.v12.common.types.EventAttribute]):
            Optional. Advertiser defined events and their
            attributes. All the values in the nested fields
            are required. Currently this field is in beta.
    """

    lifetime_value_micros = proto.Field(proto.INT64, number=1, optional=True,)
    lifetime_value_bucket = proto.Field(proto.INT32, number=2, optional=True,)
    last_purchase_date_time = proto.Field(proto.STRING, number=3,)
    average_purchase_count = proto.Field(proto.INT32, number=4,)
    average_purchase_value_micros = proto.Field(proto.INT64, number=5,)
    acquisition_date_time = proto.Field(proto.STRING, number=6,)
    shopping_loyalty = proto.Field(
        proto.MESSAGE, number=7, optional=True, message="ShoppingLoyalty",
    )
    lifecycle_stage = proto.Field(proto.STRING, number=8,)
    first_purchase_date_time = proto.Field(proto.STRING, number=9,)
    event_attribute = proto.RepeatedField(
        proto.MESSAGE, number=10, message="EventAttribute",
    )


class EventAttribute(proto.Message):
    r"""Advertiser defined events and their attributes. All the
    values in the nested fields are required.

    Attributes:
        event (str):
            Required. Advertiser defined event to be used
            for remarketing. The accepted values are
            "Viewed", "Cart", "Purchased" and "Recommended".
        event_date_time (str):
            Required. Timestamp at which the event happened. The format
            is YYYY-MM-DD HH:MM:SS[+/-HH:MM], where [+/-HH:MM] is an
            optional timezone offset from UTC. If the offset is absent,
            the API will use the account's timezone as default.
        item_attribute (Sequence[google.ads.googleads.v12.common.types.EventItemAttribute]):
            Required. Item attributes of the event.
    """

    event = proto.Field(proto.STRING, number=1,)
    event_date_time = proto.Field(proto.STRING, number=2,)
    item_attribute = proto.RepeatedField(
        proto.MESSAGE, number=3, message="EventItemAttribute",
    )


class EventItemAttribute(proto.Message):
    r"""Event Item attributes of the Customer Match.

    Attributes:
        item_id (str):
            Optional. A unique identifier of a product.
            It can be either the Merchant Center Item ID or
            GTIN (Global Trade Item Number).
    """

    item_id = proto.Field(proto.STRING, number=1,)


class ShoppingLoyalty(proto.Message):
    r"""The shopping loyalty related data. Shopping utilizes this
    data to provide users with a better experience.
    Accessible only to merchants on the allow-list.

    Attributes:
        loyalty_tier (str):
            The membership tier. It is a free-form string
            as each merchant may have their own loyalty
            system. For example, it could be a number from 1
            to 10, or a string such as "Golden" or "Silver",
            or even empty string "".

            This field is a member of `oneof`_ ``_loyalty_tier``.
    """

    loyalty_tier = proto.Field(proto.STRING, number=1, optional=True,)


class CustomerMatchUserListMetadata(proto.Message):
    r"""Metadata for customer match user list.

    Attributes:
        user_list (str):
            The resource name of remarketing list to update data.
            Required for job of CUSTOMER_MATCH_USER_LIST type.

            This field is a member of `oneof`_ ``_user_list``.
    """

    user_list = proto.Field(proto.STRING, number=2, optional=True,)


class StoreSalesMetadata(proto.Message):
    r"""Metadata for Store Sales Direct.

    Attributes:
        loyalty_fraction (float):
            This is the fraction of all transactions that
            are identifiable (for example, associated with
            any form of customer information). Required. The
            fraction needs to be between 0 and 1 (excluding
            0).

            This field is a member of `oneof`_ ``_loyalty_fraction``.
        transaction_upload_fraction (float):
            This is the ratio of sales being uploaded
            compared to the overall sales that can be
            associated with a customer. Required. The
            fraction needs to be between 0 and 1 (excluding
            0). For example, if you upload half the sales
            that you are able to associate with a customer,
            this would be 0.5.

            This field is a member of `oneof`_ ``_transaction_upload_fraction``.
        custom_key (str):
            Name of the store sales custom variable key.
            A predefined key that can be applied to the
            transaction and then later used for custom
            segmentation in reporting.
            Accessible only to customers on the allow-list.

            This field is a member of `oneof`_ ``_custom_key``.
        third_party_metadata (google.ads.googleads.v12.common.types.StoreSalesThirdPartyMetadata):
            Metadata for a third party Store Sales
            upload.
    """

    loyalty_fraction = proto.Field(proto.DOUBLE, number=5, optional=True,)
    transaction_upload_fraction = proto.Field(
        proto.DOUBLE, number=6, optional=True,
    )
    custom_key = proto.Field(proto.STRING, number=7, optional=True,)
    third_party_metadata = proto.Field(
        proto.MESSAGE, number=3, message="StoreSalesThirdPartyMetadata",
    )


class StoreSalesThirdPartyMetadata(proto.Message):
    r"""Metadata for a third party Store Sales.
    This product is only for customers on the allow-list. Contact
    your Google business development representative for details on
    the upload configuration.

    Attributes:
        advertiser_upload_date_time (str):
            Time the advertiser uploaded the data to the
            partner. Required. The format is "YYYY-MM-DD
            HH:MM:SS". Examples: "2018-03-05 09:15:00" or
            "2018-02-01 14:34:30".

            This field is a member of `oneof`_ ``_advertiser_upload_date_time``.
        valid_transaction_fraction (float):
            The fraction of transactions that are valid.
            Invalid transactions may include invalid formats
            or values. Required.
            The fraction needs to be between 0 and 1
            (excluding 0).

            This field is a member of `oneof`_ ``_valid_transaction_fraction``.
        partner_match_fraction (float):
            The fraction of valid transactions that are
            matched to a third party assigned user ID on the
            partner side. Required.
            The fraction needs to be between 0 and 1
            (excluding 0).

            This field is a member of `oneof`_ ``_partner_match_fraction``.
        partner_upload_fraction (float):
            The fraction of valid transactions that are
            uploaded by the partner to Google.
            Required.
            The fraction needs to be between 0 and 1
            (excluding 0).

            This field is a member of `oneof`_ ``_partner_upload_fraction``.
        bridge_map_version_id (str):
            Version of partner IDs to be used for
            uploads. Required.

            This field is a member of `oneof`_ ``_bridge_map_version_id``.
        partner_id (int):
            ID of the third party partner updating the
            transaction feed.

            This field is a member of `oneof`_ ``_partner_id``.
    """

    advertiser_upload_date_time = proto.Field(
        proto.STRING, number=7, optional=True,
    )
    valid_transaction_fraction = proto.Field(
        proto.DOUBLE, number=8, optional=True,
    )
    partner_match_fraction = proto.Field(proto.DOUBLE, number=9, optional=True,)
    partner_upload_fraction = proto.Field(
        proto.DOUBLE, number=10, optional=True,
    )
    bridge_map_version_id = proto.Field(proto.STRING, number=11, optional=True,)
    partner_id = proto.Field(proto.INT64, number=12, optional=True,)


__all__ = tuple(sorted(__protobuf__.manifest))
