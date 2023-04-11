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
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"SmartCampaignSetting",},
)


class SmartCampaignSetting(proto.Message):
    r"""Settings for configuring Smart campaigns.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        resource_name (str):
            Immutable. The resource name of the Smart campaign setting.
            Smart campaign setting resource names have the form:

            ``customers/{customer_id}/smartCampaignSettings/{campaign_id}``
        campaign (str):
            Output only. The campaign to which these
            settings apply.
        phone_number (google.ads.googleads.v12.resources.types.SmartCampaignSetting.PhoneNumber):
            Phone number and country code.
        advertising_language_code (str):
            The ISO-639-1 language code to advertise in.
        final_url (str):
            The user-provided landing page URL for this
            Campaign.

            This field is a member of `oneof`_ ``landing_page``.
        ad_optimized_business_profile_setting (google.ads.googleads.v12.resources.types.SmartCampaignSetting.AdOptimizedBusinessProfileSetting):
            Settings for configuring a business profile
            optimized for ads as this campaign's landing
            page.  This campaign must be linked to a
            business profile to use this option.  For more
            information on this feature, consult
            https://support.google.com/google-ads/answer/9827068.

            This field is a member of `oneof`_ ``landing_page``.
        business_name (str):
            The name of the business.

            This field is a member of `oneof`_ ``business_setting``.
        business_profile_location (str):
            The resource name of a Business Profile location. Business
            Profile location resource names can be fetched through the
            Business Profile API and adhere to the following format:
            ``locations/{locationId}``.

            See the [Business Profile API]
            (https://developers.google.com/my-business/reference/businessinformation/rest/v1/accounts.locations)
            for additional details.

            This field is a member of `oneof`_ ``business_setting``.
    """

    class PhoneNumber(proto.Message):
        r"""Phone number and country code in smart campaign settings.

        Attributes:
            phone_number (str):
                Phone number of the smart campaign.

                This field is a member of `oneof`_ ``_phone_number``.
            country_code (str):
                Upper-case, two-letter country code as
                defined by ISO-3166.

                This field is a member of `oneof`_ ``_country_code``.
        """

        phone_number = proto.Field(proto.STRING, number=1, optional=True,)
        country_code = proto.Field(proto.STRING, number=2, optional=True,)

    class AdOptimizedBusinessProfileSetting(proto.Message):
        r"""Settings for configuring a business profile optimized for ads
        as this campaign's landing page.

        Attributes:
            include_lead_form (bool):
                Enabling a lead form on your business profile
                enables prospective customers to contact your
                business by filling out a simple form, and
                you'll receive their information through email.

                This field is a member of `oneof`_ ``_include_lead_form``.
        """

        include_lead_form = proto.Field(proto.BOOL, number=1, optional=True,)

    resource_name = proto.Field(proto.STRING, number=1,)
    campaign = proto.Field(proto.STRING, number=2,)
    phone_number = proto.Field(proto.MESSAGE, number=3, message=PhoneNumber,)
    advertising_language_code = proto.Field(proto.STRING, number=7,)
    final_url = proto.Field(proto.STRING, number=8, oneof="landing_page",)
    ad_optimized_business_profile_setting = proto.Field(
        proto.MESSAGE,
        number=9,
        oneof="landing_page",
        message=AdOptimizedBusinessProfileSetting,
    )
    business_name = proto.Field(
        proto.STRING, number=5, oneof="business_setting",
    )
    business_profile_location = proto.Field(
        proto.STRING, number=10, oneof="business_setting",
    )


__all__ = tuple(sorted(__protobuf__.manifest))
