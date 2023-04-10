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

from airflow.providers.google_vendor.googleads.v12.common.types import offline_user_data
from airflow.providers.google_vendor.googleads.v12.enums.types import conversion_environment_enum
from google.rpc import status_pb2  # type: ignore


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.services",
    marshal="google.ads.googleads.v12",
    manifest={
        "UploadClickConversionsRequest",
        "UploadClickConversionsResponse",
        "UploadCallConversionsRequest",
        "UploadCallConversionsResponse",
        "ClickConversion",
        "CallConversion",
        "ExternalAttributionData",
        "ClickConversionResult",
        "CallConversionResult",
        "CustomVariable",
        "CartData",
    },
)


class UploadClickConversionsRequest(proto.Message):
    r"""Request message for
    [ConversionUploadService.UploadClickConversions][google.ads.googleads.v12.services.ConversionUploadService.UploadClickConversions].

    Attributes:
        customer_id (str):
            Required. The ID of the customer performing
            the upload.
        conversions (Sequence[google.ads.googleads.v12.services.types.ClickConversion]):
            Required. The conversions that are being
            uploaded.
        partial_failure (bool):
            Required. If true, successful operations will
            be carried out and invalid operations will
            return errors. If false, all operations will be
            carried out in one transaction if and only if
            they are all valid. This should always be set to
            true.
            See
            https://developers.google.com/google-ads/api/docs/best-practices/partial-failures
            for more information about partial failure.
        validate_only (bool):
            If true, the request is validated but not
            executed. Only errors are returned, not results.
        debug_enabled (bool):
            If true, the API will perform all upload checks and return
            errors if any are found. If false, it will perform only
            basic input validation, skip subsequent upload checks, and
            return success even if no click was found for the provided
            ``user_identifiers``.

            This setting only affects Enhanced conversions for leads
            uploads that use ``user_identifiers`` instead of ``GCLID``,
            ``GBRAID``, or ``WBRAID``. When uploading enhanced
            conversions for leads, you should upload all conversion
            events to the API, including those that may not come from
            Google Ads campaigns. The upload of an event that is not
            from a Google Ads campaign will result in a
            ``CLICK_NOT_FOUND`` error if this field is set to ``true``.
            Since these errors are expected for such events, set this
            field to ``false`` so you can confirm your uploads are
            properly formatted but ignore ``CLICK_NOT_FOUND`` errors
            from all of the conversions that are not from a Google Ads
            campaign. This will allow you to focus only on errors that
            you can address.

            Default is false.
    """

    customer_id = proto.Field(proto.STRING, number=1,)
    conversions = proto.RepeatedField(
        proto.MESSAGE, number=2, message="ClickConversion",
    )
    partial_failure = proto.Field(proto.BOOL, number=3,)
    validate_only = proto.Field(proto.BOOL, number=4,)
    debug_enabled = proto.Field(proto.BOOL, number=5,)


class UploadClickConversionsResponse(proto.Message):
    r"""Response message for
    [ConversionUploadService.UploadClickConversions][google.ads.googleads.v12.services.ConversionUploadService.UploadClickConversions].

    Attributes:
        partial_failure_error (google.rpc.status_pb2.Status):
            Errors that pertain to conversion failures in
            the partial failure mode. Returned when all
            errors occur inside the conversions. If any
            errors occur outside the conversions (for
            example, auth errors), we return an RPC level
            error. See
            https://developers.google.com/google-ads/api/docs/best-practices/partial-failures
            for more information about partial failure.
        results (Sequence[google.ads.googleads.v12.services.types.ClickConversionResult]):
            Returned for successfully processed conversions. Proto will
            be empty for rows that received an error. Results are not
            returned when validate_only is true.
    """

    partial_failure_error = proto.Field(
        proto.MESSAGE, number=1, message=status_pb2.Status,
    )
    results = proto.RepeatedField(
        proto.MESSAGE, number=2, message="ClickConversionResult",
    )


class UploadCallConversionsRequest(proto.Message):
    r"""Request message for
    [ConversionUploadService.UploadCallConversions][google.ads.googleads.v12.services.ConversionUploadService.UploadCallConversions].

    Attributes:
        customer_id (str):
            Required. The ID of the customer performing
            the upload.
        conversions (Sequence[google.ads.googleads.v12.services.types.CallConversion]):
            Required. The conversions that are being
            uploaded.
        partial_failure (bool):
            Required. If true, successful operations will
            be carried out and invalid operations will
            return errors. If false, all operations will be
            carried out in one transaction if and only if
            they are all valid. This should always be set to
            true.
            See
            https://developers.google.com/google-ads/api/docs/best-practices/partial-failures
            for more information about partial failure.
        validate_only (bool):
            If true, the request is validated but not
            executed. Only errors are returned, not results.
    """

    customer_id = proto.Field(proto.STRING, number=1,)
    conversions = proto.RepeatedField(
        proto.MESSAGE, number=2, message="CallConversion",
    )
    partial_failure = proto.Field(proto.BOOL, number=3,)
    validate_only = proto.Field(proto.BOOL, number=4,)


class UploadCallConversionsResponse(proto.Message):
    r"""Response message for
    [ConversionUploadService.UploadCallConversions][google.ads.googleads.v12.services.ConversionUploadService.UploadCallConversions].

    Attributes:
        partial_failure_error (google.rpc.status_pb2.Status):
            Errors that pertain to conversion failures in
            the partial failure mode. Returned when all
            errors occur inside the conversions. If any
            errors occur outside the conversions (for
            example, auth errors), we return an RPC level
            error. See
            https://developers.google.com/google-ads/api/docs/best-practices/partial-failures
            for more information about partial failure.
        results (Sequence[google.ads.googleads.v12.services.types.CallConversionResult]):
            Returned for successfully processed conversions. Proto will
            be empty for rows that received an error. Results are not
            returned when validate_only is true.
    """

    partial_failure_error = proto.Field(
        proto.MESSAGE, number=1, message=status_pb2.Status,
    )
    results = proto.RepeatedField(
        proto.MESSAGE, number=2, message="CallConversionResult",
    )


class ClickConversion(proto.Message):
    r"""A click conversion.

    Attributes:
        gclid (str):
            The Google click ID (gclid) associated with
            this conversion.

            This field is a member of `oneof`_ ``_gclid``.
        gbraid (str):
            The click identifier for clicks associated
            with app conversions and originating from iOS
            devices starting with iOS14.
        wbraid (str):
            The click identifier for clicks associated
            with web conversions and originating from iOS
            devices starting with iOS14.
        conversion_action (str):
            Resource name of the conversion action
            associated with this conversion. Note: Although
            this resource name consists of a customer id and
            a conversion action id, validation will ignore
            the customer id and use the conversion action id
            as the sole identifier of the conversion action.

            This field is a member of `oneof`_ ``_conversion_action``.
        conversion_date_time (str):
            The date time at which the conversion occurred. Must be
            after the click time. The timezone must be specified. The
            format is "yyyy-mm-dd hh:mm:ss+|-hh:mm", for example,
            "2019-01-01 12:32:45-08:00".

            This field is a member of `oneof`_ ``_conversion_date_time``.
        conversion_value (float):
            The value of the conversion for the
            advertiser.

            This field is a member of `oneof`_ ``_conversion_value``.
        currency_code (str):
            Currency associated with the conversion
            value. This is the ISO 4217 3-character currency
            code. For example: USD, EUR.

            This field is a member of `oneof`_ ``_currency_code``.
        order_id (str):
            The order ID associated with the conversion.
            An order id can only be used for one conversion
            per conversion action.

            This field is a member of `oneof`_ ``_order_id``.
        external_attribution_data (google.ads.googleads.v12.services.types.ExternalAttributionData):
            Additional data about externally attributed
            conversions. This field is required for
            conversions with an externally attributed
            conversion action, but should not be set
            otherwise.
        custom_variables (Sequence[google.ads.googleads.v12.services.types.CustomVariable]):
            The custom variables associated with this
            conversion.
        cart_data (google.ads.googleads.v12.services.types.CartData):
            The cart data associated with this
            conversion.
        user_identifiers (Sequence[google.ads.googleads.v12.common.types.UserIdentifier]):
            The user identifiers associated with this conversion. Only
            hashed_email and hashed_phone_number are supported for
            conversion uploads. The maximum number of user identifiers
            for each conversion is 5.
        conversion_environment (google.ads.googleads.v12.enums.types.ConversionEnvironmentEnum.ConversionEnvironment):
            The environment this conversion was recorded
            on, for example, App or Web.
    """

    gclid = proto.Field(proto.STRING, number=9, optional=True,)
    gbraid = proto.Field(proto.STRING, number=18,)
    wbraid = proto.Field(proto.STRING, number=19,)
    conversion_action = proto.Field(proto.STRING, number=10, optional=True,)
    conversion_date_time = proto.Field(proto.STRING, number=11, optional=True,)
    conversion_value = proto.Field(proto.DOUBLE, number=12, optional=True,)
    currency_code = proto.Field(proto.STRING, number=13, optional=True,)
    order_id = proto.Field(proto.STRING, number=14, optional=True,)
    external_attribution_data = proto.Field(
        proto.MESSAGE, number=7, message="ExternalAttributionData",
    )
    custom_variables = proto.RepeatedField(
        proto.MESSAGE, number=15, message="CustomVariable",
    )
    cart_data = proto.Field(proto.MESSAGE, number=16, message="CartData",)
    user_identifiers = proto.RepeatedField(
        proto.MESSAGE, number=17, message=offline_user_data.UserIdentifier,
    )
    conversion_environment = proto.Field(
        proto.ENUM,
        number=20,
        enum=conversion_environment_enum.ConversionEnvironmentEnum.ConversionEnvironment,
    )


class CallConversion(proto.Message):
    r"""A call conversion.

    Attributes:
        caller_id (str):
            The caller id from which this call was
            placed. Caller id is expected to be in E.164
            format with preceding '+' sign, for example,
            "+16502531234".

            This field is a member of `oneof`_ ``_caller_id``.
        call_start_date_time (str):
            The date time at which the call occurred. The timezone must
            be specified. The format is "yyyy-mm-dd hh:mm:ss+|-hh:mm",
            for example, "2019-01-01 12:32:45-08:00".

            This field is a member of `oneof`_ ``_call_start_date_time``.
        conversion_action (str):
            Resource name of the conversion action
            associated with this conversion. Note: Although
            this resource name consists of a customer id and
            a conversion action id, validation will ignore
            the customer id and use the conversion action id
            as the sole identifier of the conversion action.

            This field is a member of `oneof`_ ``_conversion_action``.
        conversion_date_time (str):
            The date time at which the conversion occurred. Must be
            after the call time. The timezone must be specified. The
            format is "yyyy-mm-dd hh:mm:ss+|-hh:mm", for example,
            "2019-01-01 12:32:45-08:00".

            This field is a member of `oneof`_ ``_conversion_date_time``.
        conversion_value (float):
            The value of the conversion for the
            advertiser.

            This field is a member of `oneof`_ ``_conversion_value``.
        currency_code (str):
            Currency associated with the conversion
            value. This is the ISO 4217 3-character currency
            code. For example: USD, EUR.

            This field is a member of `oneof`_ ``_currency_code``.
        custom_variables (Sequence[google.ads.googleads.v12.services.types.CustomVariable]):
            The custom variables associated with this
            conversion.
    """

    caller_id = proto.Field(proto.STRING, number=7, optional=True,)
    call_start_date_time = proto.Field(proto.STRING, number=8, optional=True,)
    conversion_action = proto.Field(proto.STRING, number=9, optional=True,)
    conversion_date_time = proto.Field(proto.STRING, number=10, optional=True,)
    conversion_value = proto.Field(proto.DOUBLE, number=11, optional=True,)
    currency_code = proto.Field(proto.STRING, number=12, optional=True,)
    custom_variables = proto.RepeatedField(
        proto.MESSAGE, number=13, message="CustomVariable",
    )


class ExternalAttributionData(proto.Message):
    r"""Contains additional information about externally attributed
    conversions.

    Attributes:
        external_attribution_credit (float):
            Represents the fraction of the conversion
            that is attributed to the Google Ads click.

            This field is a member of `oneof`_ ``_external_attribution_credit``.
        external_attribution_model (str):
            Specifies the attribution model name.

            This field is a member of `oneof`_ ``_external_attribution_model``.
    """

    external_attribution_credit = proto.Field(
        proto.DOUBLE, number=3, optional=True,
    )
    external_attribution_model = proto.Field(
        proto.STRING, number=4, optional=True,
    )


class ClickConversionResult(proto.Message):
    r"""Identifying information for a successfully processed
    ClickConversion.

    Attributes:
        gclid (str):
            The Google Click ID (gclid) associated with
            this conversion.

            This field is a member of `oneof`_ ``_gclid``.
        gbraid (str):
            The click identifier for clicks associated
            with app conversions and originating from iOS
            devices starting with iOS14.
        wbraid (str):
            The click identifier for clicks associated
            with web conversions and originating from iOS
            devices starting with iOS14.
        conversion_action (str):
            Resource name of the conversion action
            associated with this conversion.

            This field is a member of `oneof`_ ``_conversion_action``.
        conversion_date_time (str):
            The date time at which the conversion occurred. The format
            is "yyyy-mm-dd hh:mm:ss+|-hh:mm", for example, "2019-01-01
            12:32:45-08:00".

            This field is a member of `oneof`_ ``_conversion_date_time``.
        user_identifiers (Sequence[google.ads.googleads.v12.common.types.UserIdentifier]):
            The user identifiers associated with this conversion. Only
            hashed_email and hashed_phone_number are supported for
            conversion uploads. The maximum number of user identifiers
            for each conversion is 5.
    """

    gclid = proto.Field(proto.STRING, number=4, optional=True,)
    gbraid = proto.Field(proto.STRING, number=8,)
    wbraid = proto.Field(proto.STRING, number=9,)
    conversion_action = proto.Field(proto.STRING, number=5, optional=True,)
    conversion_date_time = proto.Field(proto.STRING, number=6, optional=True,)
    user_identifiers = proto.RepeatedField(
        proto.MESSAGE, number=7, message=offline_user_data.UserIdentifier,
    )


class CallConversionResult(proto.Message):
    r"""Identifying information for a successfully processed
    CallConversionUpload.

    Attributes:
        caller_id (str):
            The caller id from which this call was
            placed. Caller id is expected to be in E.164
            format with preceding '+' sign.

            This field is a member of `oneof`_ ``_caller_id``.
        call_start_date_time (str):
            The date time at which the call occurred. The format is
            "yyyy-mm-dd hh:mm:ss+|-hh:mm", for example, "2019-01-01
            12:32:45-08:00".

            This field is a member of `oneof`_ ``_call_start_date_time``.
        conversion_action (str):
            Resource name of the conversion action
            associated with this conversion.

            This field is a member of `oneof`_ ``_conversion_action``.
        conversion_date_time (str):
            The date time at which the conversion occurred. The format
            is "yyyy-mm-dd hh:mm:ss+|-hh:mm", for example, "2019-01-01
            12:32:45-08:00".

            This field is a member of `oneof`_ ``_conversion_date_time``.
    """

    caller_id = proto.Field(proto.STRING, number=5, optional=True,)
    call_start_date_time = proto.Field(proto.STRING, number=6, optional=True,)
    conversion_action = proto.Field(proto.STRING, number=7, optional=True,)
    conversion_date_time = proto.Field(proto.STRING, number=8, optional=True,)


class CustomVariable(proto.Message):
    r"""A custom variable.

    Attributes:
        conversion_custom_variable (str):
            Resource name of the custom variable
            associated with this conversion. Note: Although
            this resource name consists of a customer id and
            a conversion custom variable id, validation will
            ignore the customer id and use the conversion
            custom variable id as the sole identifier of the
            conversion custom variable.
        value (str):
            The value string of this custom variable.
            The value of the custom variable should not
            contain private customer data, such as email
            addresses or phone numbers.
    """

    conversion_custom_variable = proto.Field(proto.STRING, number=1,)
    value = proto.Field(proto.STRING, number=2,)


class CartData(proto.Message):
    r"""Contains additional information about cart data.

    Attributes:
        merchant_id (int):
            The Merchant Center ID where the items are
            uploaded.
        feed_country_code (str):
            The country code associated with the feed
            where the items are uploaded.
        feed_language_code (str):
            The language code associated with the feed
            where the items are uploaded.
        local_transaction_cost (float):
            Sum of all transaction level discounts, such
            as free shipping and coupon discounts for the
            whole cart. The currency code is the same as
            that in the ClickConversion message.
        items (Sequence[google.ads.googleads.v12.services.types.CartData.Item]):
            Data of the items purchased.
    """

    class Item(proto.Message):
        r"""Contains data of the items purchased.

        Attributes:
            product_id (str):
                The shopping id of the item. Must be equal to
                the Merchant Center product identifier.
            quantity (int):
                Number of items sold.
            unit_price (float):
                Unit price excluding tax, shipping, and any
                transaction level discounts. The currency code
                is the same as that in the ClickConversion
                message.
        """

        product_id = proto.Field(proto.STRING, number=1,)
        quantity = proto.Field(proto.INT32, number=2,)
        unit_price = proto.Field(proto.DOUBLE, number=3,)

    merchant_id = proto.Field(proto.INT64, number=6,)
    feed_country_code = proto.Field(proto.STRING, number=2,)
    feed_language_code = proto.Field(proto.STRING, number=3,)
    local_transaction_cost = proto.Field(proto.DOUBLE, number=4,)
    items = proto.RepeatedField(proto.MESSAGE, number=5, message=Item,)


__all__ = tuple(sorted(__protobuf__.manifest))
