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

from airflow.providers.google_vendor.googleads.v12.enums.types import ad_customizer_placeholder_field
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    affiliate_location_placeholder_field,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import app_placeholder_field
from airflow.providers.google_vendor.googleads.v12.enums.types import call_placeholder_field
from airflow.providers.google_vendor.googleads.v12.enums.types import callout_placeholder_field
from airflow.providers.google_vendor.googleads.v12.enums.types import custom_placeholder_field
from airflow.providers.google_vendor.googleads.v12.enums.types import dsa_page_feed_criterion_field
from airflow.providers.google_vendor.googleads.v12.enums.types import education_placeholder_field
from airflow.providers.google_vendor.googleads.v12.enums.types import feed_mapping_criterion_type
from airflow.providers.google_vendor.googleads.v12.enums.types import feed_mapping_status
from airflow.providers.google_vendor.googleads.v12.enums.types import flight_placeholder_field
from airflow.providers.google_vendor.googleads.v12.enums.types import hotel_placeholder_field
from airflow.providers.google_vendor.googleads.v12.enums.types import image_placeholder_field
from airflow.providers.google_vendor.googleads.v12.enums.types import job_placeholder_field
from airflow.providers.google_vendor.googleads.v12.enums.types import local_placeholder_field
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    location_extension_targeting_criterion_field,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import location_placeholder_field
from airflow.providers.google_vendor.googleads.v12.enums.types import message_placeholder_field
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    placeholder_type as gage_placeholder_type,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import price_placeholder_field
from airflow.providers.google_vendor.googleads.v12.enums.types import promotion_placeholder_field
from airflow.providers.google_vendor.googleads.v12.enums.types import real_estate_placeholder_field
from airflow.providers.google_vendor.googleads.v12.enums.types import sitelink_placeholder_field
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    structured_snippet_placeholder_field,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import travel_placeholder_field


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"FeedMapping", "AttributeFieldMapping",},
)


class FeedMapping(proto.Message):
    r"""A feed mapping.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        resource_name (str):
            Immutable. The resource name of the feed mapping. Feed
            mapping resource names have the form:

            ``customers/{customer_id}/feedMappings/{feed_id}~{feed_mapping_id}``
        feed (str):
            Immutable. The feed of this feed mapping.

            This field is a member of `oneof`_ ``_feed``.
        attribute_field_mappings (Sequence[google.ads.googleads.v12.resources.types.AttributeFieldMapping]):
            Immutable. Feed attributes to field mappings.
            These mappings are a one-to-many relationship
            meaning that 1 feed attribute can be used to
            populate multiple placeholder fields, but 1
            placeholder field can only draw data from 1 feed
            attribute. Ad Customizer is an exception, 1
            placeholder field can be mapped to multiple feed
            attributes. Required.
        status (google.ads.googleads.v12.enums.types.FeedMappingStatusEnum.FeedMappingStatus):
            Output only. Status of the feed mapping.
            This field is read-only.
        placeholder_type (google.ads.googleads.v12.enums.types.PlaceholderTypeEnum.PlaceholderType):
            Immutable. The placeholder type of this
            mapping (for example, if the mapping maps feed
            attributes to placeholder fields).

            This field is a member of `oneof`_ ``target``.
        criterion_type (google.ads.googleads.v12.enums.types.FeedMappingCriterionTypeEnum.FeedMappingCriterionType):
            Immutable. The criterion type of this mapping
            (for example, if the mapping maps feed
            attributes to criterion fields).

            This field is a member of `oneof`_ ``target``.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    feed = proto.Field(proto.STRING, number=7, optional=True,)
    attribute_field_mappings = proto.RepeatedField(
        proto.MESSAGE, number=5, message="AttributeFieldMapping",
    )
    status = proto.Field(
        proto.ENUM,
        number=6,
        enum=feed_mapping_status.FeedMappingStatusEnum.FeedMappingStatus,
    )
    placeholder_type = proto.Field(
        proto.ENUM,
        number=3,
        oneof="target",
        enum=gage_placeholder_type.PlaceholderTypeEnum.PlaceholderType,
    )
    criterion_type = proto.Field(
        proto.ENUM,
        number=4,
        oneof="target",
        enum=feed_mapping_criterion_type.FeedMappingCriterionTypeEnum.FeedMappingCriterionType,
    )


class AttributeFieldMapping(proto.Message):
    r"""Maps from feed attribute id to a placeholder or criterion
    field id.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        feed_attribute_id (int):
            Immutable. Feed attribute from which to map.

            This field is a member of `oneof`_ ``_feed_attribute_id``.
        field_id (int):
            Output only. The placeholder field ID. If a
            placeholder field enum is not published in the
            current API version, then this field will be
            populated and the field oneof will be empty.
            This field is read-only.

            This field is a member of `oneof`_ ``_field_id``.
        sitelink_field (google.ads.googleads.v12.enums.types.SitelinkPlaceholderFieldEnum.SitelinkPlaceholderField):
            Immutable. Sitelink Placeholder Fields.

            This field is a member of `oneof`_ ``field``.
        call_field (google.ads.googleads.v12.enums.types.CallPlaceholderFieldEnum.CallPlaceholderField):
            Immutable. Call Placeholder Fields.

            This field is a member of `oneof`_ ``field``.
        app_field (google.ads.googleads.v12.enums.types.AppPlaceholderFieldEnum.AppPlaceholderField):
            Immutable. App Placeholder Fields.

            This field is a member of `oneof`_ ``field``.
        location_field (google.ads.googleads.v12.enums.types.LocationPlaceholderFieldEnum.LocationPlaceholderField):
            Output only. Location Placeholder Fields.
            This field is read-only.

            This field is a member of `oneof`_ ``field``.
        affiliate_location_field (google.ads.googleads.v12.enums.types.AffiliateLocationPlaceholderFieldEnum.AffiliateLocationPlaceholderField):
            Output only. Affiliate Location Placeholder
            Fields. This field is read-only.

            This field is a member of `oneof`_ ``field``.
        callout_field (google.ads.googleads.v12.enums.types.CalloutPlaceholderFieldEnum.CalloutPlaceholderField):
            Immutable. Callout Placeholder Fields.

            This field is a member of `oneof`_ ``field``.
        structured_snippet_field (google.ads.googleads.v12.enums.types.StructuredSnippetPlaceholderFieldEnum.StructuredSnippetPlaceholderField):
            Immutable. Structured Snippet Placeholder
            Fields.

            This field is a member of `oneof`_ ``field``.
        message_field (google.ads.googleads.v12.enums.types.MessagePlaceholderFieldEnum.MessagePlaceholderField):
            Immutable. Message Placeholder Fields.

            This field is a member of `oneof`_ ``field``.
        price_field (google.ads.googleads.v12.enums.types.PricePlaceholderFieldEnum.PricePlaceholderField):
            Immutable. Price Placeholder Fields.

            This field is a member of `oneof`_ ``field``.
        promotion_field (google.ads.googleads.v12.enums.types.PromotionPlaceholderFieldEnum.PromotionPlaceholderField):
            Immutable. Promotion Placeholder Fields.

            This field is a member of `oneof`_ ``field``.
        ad_customizer_field (google.ads.googleads.v12.enums.types.AdCustomizerPlaceholderFieldEnum.AdCustomizerPlaceholderField):
            Immutable. Ad Customizer Placeholder Fields

            This field is a member of `oneof`_ ``field``.
        dsa_page_feed_field (google.ads.googleads.v12.enums.types.DsaPageFeedCriterionFieldEnum.DsaPageFeedCriterionField):
            Immutable. Dynamic Search Ad Page Feed
            Fields.

            This field is a member of `oneof`_ ``field``.
        location_extension_targeting_field (google.ads.googleads.v12.enums.types.LocationExtensionTargetingCriterionFieldEnum.LocationExtensionTargetingCriterionField):
            Immutable. Location Target Fields.

            This field is a member of `oneof`_ ``field``.
        education_field (google.ads.googleads.v12.enums.types.EducationPlaceholderFieldEnum.EducationPlaceholderField):
            Immutable. Education Placeholder Fields

            This field is a member of `oneof`_ ``field``.
        flight_field (google.ads.googleads.v12.enums.types.FlightPlaceholderFieldEnum.FlightPlaceholderField):
            Immutable. Flight Placeholder Fields

            This field is a member of `oneof`_ ``field``.
        custom_field (google.ads.googleads.v12.enums.types.CustomPlaceholderFieldEnum.CustomPlaceholderField):
            Immutable. Custom Placeholder Fields

            This field is a member of `oneof`_ ``field``.
        hotel_field (google.ads.googleads.v12.enums.types.HotelPlaceholderFieldEnum.HotelPlaceholderField):
            Immutable. Hotel Placeholder Fields

            This field is a member of `oneof`_ ``field``.
        real_estate_field (google.ads.googleads.v12.enums.types.RealEstatePlaceholderFieldEnum.RealEstatePlaceholderField):
            Immutable. Real Estate Placeholder Fields

            This field is a member of `oneof`_ ``field``.
        travel_field (google.ads.googleads.v12.enums.types.TravelPlaceholderFieldEnum.TravelPlaceholderField):
            Immutable. Travel Placeholder Fields

            This field is a member of `oneof`_ ``field``.
        local_field (google.ads.googleads.v12.enums.types.LocalPlaceholderFieldEnum.LocalPlaceholderField):
            Immutable. Local Placeholder Fields

            This field is a member of `oneof`_ ``field``.
        job_field (google.ads.googleads.v12.enums.types.JobPlaceholderFieldEnum.JobPlaceholderField):
            Immutable. Job Placeholder Fields

            This field is a member of `oneof`_ ``field``.
        image_field (google.ads.googleads.v12.enums.types.ImagePlaceholderFieldEnum.ImagePlaceholderField):
            Immutable. Image Placeholder Fields

            This field is a member of `oneof`_ ``field``.
    """

    feed_attribute_id = proto.Field(proto.INT64, number=24, optional=True,)
    field_id = proto.Field(proto.INT64, number=25, optional=True,)
    sitelink_field = proto.Field(
        proto.ENUM,
        number=3,
        oneof="field",
        enum=sitelink_placeholder_field.SitelinkPlaceholderFieldEnum.SitelinkPlaceholderField,
    )
    call_field = proto.Field(
        proto.ENUM,
        number=4,
        oneof="field",
        enum=call_placeholder_field.CallPlaceholderFieldEnum.CallPlaceholderField,
    )
    app_field = proto.Field(
        proto.ENUM,
        number=5,
        oneof="field",
        enum=app_placeholder_field.AppPlaceholderFieldEnum.AppPlaceholderField,
    )
    location_field = proto.Field(
        proto.ENUM,
        number=6,
        oneof="field",
        enum=location_placeholder_field.LocationPlaceholderFieldEnum.LocationPlaceholderField,
    )
    affiliate_location_field = proto.Field(
        proto.ENUM,
        number=7,
        oneof="field",
        enum=affiliate_location_placeholder_field.AffiliateLocationPlaceholderFieldEnum.AffiliateLocationPlaceholderField,
    )
    callout_field = proto.Field(
        proto.ENUM,
        number=8,
        oneof="field",
        enum=callout_placeholder_field.CalloutPlaceholderFieldEnum.CalloutPlaceholderField,
    )
    structured_snippet_field = proto.Field(
        proto.ENUM,
        number=9,
        oneof="field",
        enum=structured_snippet_placeholder_field.StructuredSnippetPlaceholderFieldEnum.StructuredSnippetPlaceholderField,
    )
    message_field = proto.Field(
        proto.ENUM,
        number=10,
        oneof="field",
        enum=message_placeholder_field.MessagePlaceholderFieldEnum.MessagePlaceholderField,
    )
    price_field = proto.Field(
        proto.ENUM,
        number=11,
        oneof="field",
        enum=price_placeholder_field.PricePlaceholderFieldEnum.PricePlaceholderField,
    )
    promotion_field = proto.Field(
        proto.ENUM,
        number=12,
        oneof="field",
        enum=promotion_placeholder_field.PromotionPlaceholderFieldEnum.PromotionPlaceholderField,
    )
    ad_customizer_field = proto.Field(
        proto.ENUM,
        number=13,
        oneof="field",
        enum=ad_customizer_placeholder_field.AdCustomizerPlaceholderFieldEnum.AdCustomizerPlaceholderField,
    )
    dsa_page_feed_field = proto.Field(
        proto.ENUM,
        number=14,
        oneof="field",
        enum=dsa_page_feed_criterion_field.DsaPageFeedCriterionFieldEnum.DsaPageFeedCriterionField,
    )
    location_extension_targeting_field = proto.Field(
        proto.ENUM,
        number=15,
        oneof="field",
        enum=location_extension_targeting_criterion_field.LocationExtensionTargetingCriterionFieldEnum.LocationExtensionTargetingCriterionField,
    )
    education_field = proto.Field(
        proto.ENUM,
        number=16,
        oneof="field",
        enum=education_placeholder_field.EducationPlaceholderFieldEnum.EducationPlaceholderField,
    )
    flight_field = proto.Field(
        proto.ENUM,
        number=17,
        oneof="field",
        enum=flight_placeholder_field.FlightPlaceholderFieldEnum.FlightPlaceholderField,
    )
    custom_field = proto.Field(
        proto.ENUM,
        number=18,
        oneof="field",
        enum=custom_placeholder_field.CustomPlaceholderFieldEnum.CustomPlaceholderField,
    )
    hotel_field = proto.Field(
        proto.ENUM,
        number=19,
        oneof="field",
        enum=hotel_placeholder_field.HotelPlaceholderFieldEnum.HotelPlaceholderField,
    )
    real_estate_field = proto.Field(
        proto.ENUM,
        number=20,
        oneof="field",
        enum=real_estate_placeholder_field.RealEstatePlaceholderFieldEnum.RealEstatePlaceholderField,
    )
    travel_field = proto.Field(
        proto.ENUM,
        number=21,
        oneof="field",
        enum=travel_placeholder_field.TravelPlaceholderFieldEnum.TravelPlaceholderField,
    )
    local_field = proto.Field(
        proto.ENUM,
        number=22,
        oneof="field",
        enum=local_placeholder_field.LocalPlaceholderFieldEnum.LocalPlaceholderField,
    )
    job_field = proto.Field(
        proto.ENUM,
        number=23,
        oneof="field",
        enum=job_placeholder_field.JobPlaceholderFieldEnum.JobPlaceholderField,
    )
    image_field = proto.Field(
        proto.ENUM,
        number=26,
        oneof="field",
        enum=image_placeholder_field.ImagePlaceholderFieldEnum.ImagePlaceholderField,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
