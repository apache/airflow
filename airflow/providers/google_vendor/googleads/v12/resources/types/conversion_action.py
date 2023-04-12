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

from airflow.providers.google_vendor.googleads.v12.common.types import tag_snippet
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    attribution_model as gage_attribution_model,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import conversion_action_category
from airflow.providers.google_vendor.googleads.v12.enums.types import conversion_action_counting_type
from airflow.providers.google_vendor.googleads.v12.enums.types import conversion_action_status
from airflow.providers.google_vendor.googleads.v12.enums.types import conversion_action_type
from airflow.providers.google_vendor.googleads.v12.enums.types import conversion_origin
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    data_driven_model_status as gage_data_driven_model_status,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    mobile_app_vendor as gage_mobile_app_vendor,
)


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"ConversionAction",},
)


class ConversionAction(proto.Message):
    r"""A conversion action.

    Attributes:
        resource_name (str):
            Immutable. The resource name of the conversion action.
            Conversion action resource names have the form:

            ``customers/{customer_id}/conversionActions/{conversion_action_id}``
        id (int):
            Output only. The ID of the conversion action.

            This field is a member of `oneof`_ ``_id``.
        name (str):
            The name of the conversion action.
            This field is required and should not be empty
            when creating new conversion actions.

            This field is a member of `oneof`_ ``_name``.
        status (google.ads.googleads.v12.enums.types.ConversionActionStatusEnum.ConversionActionStatus):
            The status of this conversion action for
            conversion event accrual.
        type_ (google.ads.googleads.v12.enums.types.ConversionActionTypeEnum.ConversionActionType):
            Immutable. The type of this conversion
            action.
        origin (google.ads.googleads.v12.enums.types.ConversionOriginEnum.ConversionOrigin):
            Output only. The conversion origin of this
            conversion action.
        primary_for_goal (bool):
            If a conversion action's primary_for_goal bit is false, the
            conversion action is non-biddable for all campaigns
            regardless of their customer conversion goal or campaign
            conversion goal. However, custom conversion goals do not
            respect primary_for_goal, so if a campaign has a custom
            conversion goal configured with a primary_for_goal = false
            conversion action, that conversion action is still biddable.
            By default, primary_for_goal will be true if not set. In V9,
            primary_for_goal can only be set to false after creation
            through an 'update' operation because it's not declared as
            optional.

            This field is a member of `oneof`_ ``_primary_for_goal``.
        category (google.ads.googleads.v12.enums.types.ConversionActionCategoryEnum.ConversionActionCategory):
            The category of conversions reported for this
            conversion action.
        owner_customer (str):
            Output only. The resource name of the
            conversion action owner customer, or null if
            this is a system-defined conversion action.

            This field is a member of `oneof`_ ``_owner_customer``.
        include_in_conversions_metric (bool):
            Whether this conversion action should be
            included in the "conversions" metric.

            This field is a member of `oneof`_ ``_include_in_conversions_metric``.
        click_through_lookback_window_days (int):
            The maximum number of days that may elapse
            between an interaction (for example, a click)
            and a conversion event.

            This field is a member of `oneof`_ ``_click_through_lookback_window_days``.
        view_through_lookback_window_days (int):
            The maximum number of days which may elapse
            between an impression and a conversion without
            an interaction.

            This field is a member of `oneof`_ ``_view_through_lookback_window_days``.
        value_settings (google.ads.googleads.v12.resources.types.ConversionAction.ValueSettings):
            Settings related to the value for conversion
            events associated with this conversion action.
        counting_type (google.ads.googleads.v12.enums.types.ConversionActionCountingTypeEnum.ConversionActionCountingType):
            How to count conversion events for the
            conversion action.
        attribution_model_settings (google.ads.googleads.v12.resources.types.ConversionAction.AttributionModelSettings):
            Settings related to this conversion action's
            attribution model.
        tag_snippets (Sequence[google.ads.googleads.v12.common.types.TagSnippet]):
            Output only. The snippets used for tracking
            conversions.
        phone_call_duration_seconds (int):
            The phone call duration in seconds after
            which a conversion should be reported for this
            conversion action.
            The value must be between 0 and 10000,
            inclusive.

            This field is a member of `oneof`_ ``_phone_call_duration_seconds``.
        app_id (str):
            App ID for an app conversion action.

            This field is a member of `oneof`_ ``_app_id``.
        mobile_app_vendor (google.ads.googleads.v12.enums.types.MobileAppVendorEnum.MobileAppVendor):
            Output only. Mobile app vendor for an app
            conversion action.
        firebase_settings (google.ads.googleads.v12.resources.types.ConversionAction.FirebaseSettings):
            Output only. Firebase settings for Firebase
            conversion types.
        third_party_app_analytics_settings (google.ads.googleads.v12.resources.types.ConversionAction.ThirdPartyAppAnalyticsSettings):
            Output only. Third Party App Analytics
            settings for third party conversion types.
    """

    class AttributionModelSettings(proto.Message):
        r"""Settings related to this conversion action's attribution
        model.

        Attributes:
            attribution_model (google.ads.googleads.v12.enums.types.AttributionModelEnum.AttributionModel):
                The attribution model type of this conversion
                action.
            data_driven_model_status (google.ads.googleads.v12.enums.types.DataDrivenModelStatusEnum.DataDrivenModelStatus):
                Output only. The status of the data-driven
                attribution model for the conversion action.
        """

        attribution_model = proto.Field(
            proto.ENUM,
            number=1,
            enum=gage_attribution_model.AttributionModelEnum.AttributionModel,
        )
        data_driven_model_status = proto.Field(
            proto.ENUM,
            number=2,
            enum=gage_data_driven_model_status.DataDrivenModelStatusEnum.DataDrivenModelStatus,
        )

    class ValueSettings(proto.Message):
        r"""Settings related to the value for conversion events
        associated with this conversion action.

        Attributes:
            default_value (float):
                The value to use when conversion events for
                this conversion action are sent with an invalid,
                disallowed or missing value, or when this
                conversion action is configured to always use
                the default value.

                This field is a member of `oneof`_ ``_default_value``.
            default_currency_code (str):
                The currency code to use when conversion
                events for this conversion action are sent with
                an invalid or missing currency code, or when
                this conversion action is configured to always
                use the default value.

                This field is a member of `oneof`_ ``_default_currency_code``.
            always_use_default_value (bool):
                Controls whether the default value and
                default currency code are used in place of the
                value and currency code specified in conversion
                events for this conversion action.

                This field is a member of `oneof`_ ``_always_use_default_value``.
        """

        default_value = proto.Field(proto.DOUBLE, number=4, optional=True,)
        default_currency_code = proto.Field(
            proto.STRING, number=5, optional=True,
        )
        always_use_default_value = proto.Field(
            proto.BOOL, number=6, optional=True,
        )

    class ThirdPartyAppAnalyticsSettings(proto.Message):
        r"""Settings related to a third party app analytics conversion
        action.

        Attributes:
            event_name (str):
                Output only. The event name of a third-party
                app analytics conversion.

                This field is a member of `oneof`_ ``_event_name``.
            provider_name (str):
                Output only. Name of the third-party app
                analytics provider.
        """

        event_name = proto.Field(proto.STRING, number=2, optional=True,)
        provider_name = proto.Field(proto.STRING, number=3,)

    class FirebaseSettings(proto.Message):
        r"""Settings related to a Firebase conversion action.

        Attributes:
            event_name (str):
                Output only. The event name of a Firebase
                conversion.

                This field is a member of `oneof`_ ``_event_name``.
            project_id (str):
                Output only. The Firebase project ID of the
                conversion.

                This field is a member of `oneof`_ ``_project_id``.
            property_id (int):
                Output only. The GA property ID of the
                conversion.
            property_name (str):
                Output only. The GA property name of the
                conversion.
        """

        event_name = proto.Field(proto.STRING, number=3, optional=True,)
        project_id = proto.Field(proto.STRING, number=4, optional=True,)
        property_id = proto.Field(proto.INT64, number=5,)
        property_name = proto.Field(proto.STRING, number=6,)

    resource_name = proto.Field(proto.STRING, number=1,)
    id = proto.Field(proto.INT64, number=21, optional=True,)
    name = proto.Field(proto.STRING, number=22, optional=True,)
    status = proto.Field(
        proto.ENUM,
        number=4,
        enum=conversion_action_status.ConversionActionStatusEnum.ConversionActionStatus,
    )
    type_ = proto.Field(
        proto.ENUM,
        number=5,
        enum=conversion_action_type.ConversionActionTypeEnum.ConversionActionType,
    )
    origin = proto.Field(
        proto.ENUM,
        number=30,
        enum=conversion_origin.ConversionOriginEnum.ConversionOrigin,
    )
    primary_for_goal = proto.Field(proto.BOOL, number=31, optional=True,)
    category = proto.Field(
        proto.ENUM,
        number=6,
        enum=conversion_action_category.ConversionActionCategoryEnum.ConversionActionCategory,
    )
    owner_customer = proto.Field(proto.STRING, number=23, optional=True,)
    include_in_conversions_metric = proto.Field(
        proto.BOOL, number=24, optional=True,
    )
    click_through_lookback_window_days = proto.Field(
        proto.INT64, number=25, optional=True,
    )
    view_through_lookback_window_days = proto.Field(
        proto.INT64, number=26, optional=True,
    )
    value_settings = proto.Field(
        proto.MESSAGE, number=11, message=ValueSettings,
    )
    counting_type = proto.Field(
        proto.ENUM,
        number=12,
        enum=conversion_action_counting_type.ConversionActionCountingTypeEnum.ConversionActionCountingType,
    )
    attribution_model_settings = proto.Field(
        proto.MESSAGE, number=13, message=AttributionModelSettings,
    )
    tag_snippets = proto.RepeatedField(
        proto.MESSAGE, number=14, message=tag_snippet.TagSnippet,
    )
    phone_call_duration_seconds = proto.Field(
        proto.INT64, number=27, optional=True,
    )
    app_id = proto.Field(proto.STRING, number=28, optional=True,)
    mobile_app_vendor = proto.Field(
        proto.ENUM,
        number=17,
        enum=gage_mobile_app_vendor.MobileAppVendorEnum.MobileAppVendor,
    )
    firebase_settings = proto.Field(
        proto.MESSAGE, number=18, message=FirebaseSettings,
    )
    third_party_app_analytics_settings = proto.Field(
        proto.MESSAGE, number=19, message=ThirdPartyAppAnalyticsSettings,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
