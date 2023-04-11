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
    affiliate_location_feed_relationship_type,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import feed_attribute_type
from airflow.providers.google_vendor.googleads.v12.enums.types import feed_origin
from airflow.providers.google_vendor.googleads.v12.enums.types import feed_status


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"Feed", "FeedAttribute", "FeedAttributeOperation",},
)


class Feed(proto.Message):
    r"""A feed.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        resource_name (str):
            Immutable. The resource name of the feed. Feed resource
            names have the form:

            ``customers/{customer_id}/feeds/{feed_id}``
        id (int):
            Output only. The ID of the feed.
            This field is read-only.

            This field is a member of `oneof`_ ``_id``.
        name (str):
            Immutable. Name of the feed. Required.

            This field is a member of `oneof`_ ``_name``.
        attributes (Sequence[google.ads.googleads.v12.resources.types.FeedAttribute]):
            The Feed's attributes. Required on CREATE, unless
            system_feed_generation_data is provided, in which case
            Google Ads will update the feed with the correct attributes.
            Disallowed on UPDATE. Use attribute_operations to add new
            attributes.
        attribute_operations (Sequence[google.ads.googleads.v12.resources.types.FeedAttributeOperation]):
            The list of operations changing the feed
            attributes. Attributes can only be added, not
            removed.
        origin (google.ads.googleads.v12.enums.types.FeedOriginEnum.FeedOrigin):
            Immutable. Specifies who manages the
            FeedAttributes for the Feed.
        status (google.ads.googleads.v12.enums.types.FeedStatusEnum.FeedStatus):
            Output only. Status of the feed.
            This field is read-only.
        places_location_feed_data (google.ads.googleads.v12.resources.types.Feed.PlacesLocationFeedData):
            Data used to configure a location feed
            populated from Business Profile.

            This field is a member of `oneof`_ ``system_feed_generation_data``.
        affiliate_location_feed_data (google.ads.googleads.v12.resources.types.Feed.AffiliateLocationFeedData):
            Data used to configure an affiliate location
            feed populated with the specified chains.

            This field is a member of `oneof`_ ``system_feed_generation_data``.
    """

    class PlacesLocationFeedData(proto.Message):
        r"""Data used to configure a location feed populated from
        Business Profile.

        Attributes:
            oauth_info (google.ads.googleads.v12.resources.types.Feed.PlacesLocationFeedData.OAuthInfo):
                Immutable. Required authentication token
                (from OAuth API) for the email. This field can
                only be specified in a create request. All its
                subfields are not selectable.
            email_address (str):
                Email address of a Business Profile or email
                address of a manager of the Business Profile.
                Required.

                This field is a member of `oneof`_ ``_email_address``.
            business_account_id (str):
                Plus page ID of the managed business whose locations should
                be used. If this field is not set, then all businesses
                accessible by the user (specified by email_address) are
                used. This field is mutate-only and is not selectable.
            business_name_filter (str):
                Used to filter Business Profile listings by business name.
                If business_name_filter is set, only listings with a
                matching business name are candidates to be sync'd into
                FeedItems.

                This field is a member of `oneof`_ ``_business_name_filter``.
            category_filters (Sequence[str]):
                Used to filter Business Profile listings by categories. If
                entries exist in category_filters, only listings that belong
                to any of the categories are candidates to be sync'd into
                FeedItems. If no entries exist in category_filters, then all
                listings are candidates for syncing.
            label_filters (Sequence[str]):
                Used to filter Business Profile listings by labels. If
                entries exist in label_filters, only listings that has any
                of the labels set are candidates to be synchronized into
                FeedItems. If no entries exist in label_filters, then all
                listings are candidates for syncing.
        """

        class OAuthInfo(proto.Message):
            r"""Data used for authorization using OAuth.

            Attributes:
                http_method (str):
                    The HTTP method used to obtain authorization.

                    This field is a member of `oneof`_ ``_http_method``.
                http_request_url (str):
                    The HTTP request URL used to obtain
                    authorization.

                    This field is a member of `oneof`_ ``_http_request_url``.
                http_authorization_header (str):
                    The HTTP authorization header used to obtain
                    authorization.

                    This field is a member of `oneof`_ ``_http_authorization_header``.
            """

            http_method = proto.Field(proto.STRING, number=4, optional=True,)
            http_request_url = proto.Field(
                proto.STRING, number=5, optional=True,
            )
            http_authorization_header = proto.Field(
                proto.STRING, number=6, optional=True,
            )

        oauth_info = proto.Field(
            proto.MESSAGE,
            number=1,
            message="Feed.PlacesLocationFeedData.OAuthInfo",
        )
        email_address = proto.Field(proto.STRING, number=7, optional=True,)
        business_account_id = proto.Field(proto.STRING, number=8,)
        business_name_filter = proto.Field(
            proto.STRING, number=9, optional=True,
        )
        category_filters = proto.RepeatedField(proto.STRING, number=11,)
        label_filters = proto.RepeatedField(proto.STRING, number=12,)

    class AffiliateLocationFeedData(proto.Message):
        r"""Data used to configure an affiliate location feed populated
        with the specified chains.

        Attributes:
            chain_ids (Sequence[int]):
                The list of chains that the affiliate
                location feed will sync the locations from.
            relationship_type (google.ads.googleads.v12.enums.types.AffiliateLocationFeedRelationshipTypeEnum.AffiliateLocationFeedRelationshipType):
                The relationship the chains have with the
                advertiser.
        """

        chain_ids = proto.RepeatedField(proto.INT64, number=3,)
        relationship_type = proto.Field(
            proto.ENUM,
            number=2,
            enum=affiliate_location_feed_relationship_type.AffiliateLocationFeedRelationshipTypeEnum.AffiliateLocationFeedRelationshipType,
        )

    resource_name = proto.Field(proto.STRING, number=1,)
    id = proto.Field(proto.INT64, number=11, optional=True,)
    name = proto.Field(proto.STRING, number=12, optional=True,)
    attributes = proto.RepeatedField(
        proto.MESSAGE, number=4, message="FeedAttribute",
    )
    attribute_operations = proto.RepeatedField(
        proto.MESSAGE, number=9, message="FeedAttributeOperation",
    )
    origin = proto.Field(
        proto.ENUM, number=5, enum=feed_origin.FeedOriginEnum.FeedOrigin,
    )
    status = proto.Field(
        proto.ENUM, number=8, enum=feed_status.FeedStatusEnum.FeedStatus,
    )
    places_location_feed_data = proto.Field(
        proto.MESSAGE,
        number=6,
        oneof="system_feed_generation_data",
        message=PlacesLocationFeedData,
    )
    affiliate_location_feed_data = proto.Field(
        proto.MESSAGE,
        number=7,
        oneof="system_feed_generation_data",
        message=AffiliateLocationFeedData,
    )


class FeedAttribute(proto.Message):
    r"""FeedAttributes define the types of data expected to be
    present in a Feed. A single FeedAttribute specifies the expected
    type of the FeedItemAttributes with the same FeedAttributeId.
    Optionally, a FeedAttribute can be marked as being part of a
    FeedItem's unique key.

    Attributes:
        id (int):
            ID of the attribute.

            This field is a member of `oneof`_ ``_id``.
        name (str):
            The name of the attribute. Required.

            This field is a member of `oneof`_ ``_name``.
        type_ (google.ads.googleads.v12.enums.types.FeedAttributeTypeEnum.FeedAttributeType):
            Data type for feed attribute. Required.
        is_part_of_key (bool):
            Indicates that data corresponding to this attribute is part
            of a FeedItem's unique key. It defaults to false if it is
            unspecified. Note that a unique key is not required in a
            Feed's schema, in which case the FeedItems must be
            referenced by their feed_item_id.

            This field is a member of `oneof`_ ``_is_part_of_key``.
    """

    id = proto.Field(proto.INT64, number=5, optional=True,)
    name = proto.Field(proto.STRING, number=6, optional=True,)
    type_ = proto.Field(
        proto.ENUM,
        number=3,
        enum=feed_attribute_type.FeedAttributeTypeEnum.FeedAttributeType,
    )
    is_part_of_key = proto.Field(proto.BOOL, number=7, optional=True,)


class FeedAttributeOperation(proto.Message):
    r"""Operation to be performed on a feed attribute list in a
    mutate.

    Attributes:
        operator (google.ads.googleads.v12.resources.types.FeedAttributeOperation.Operator):
            Output only. Type of list operation to
            perform.
        value (google.ads.googleads.v12.resources.types.FeedAttribute):
            Output only. The feed attribute being added
            to the list.
    """

    class Operator(proto.Enum):
        r"""The operator."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        ADD = 2

    operator = proto.Field(proto.ENUM, number=1, enum=Operator,)
    value = proto.Field(proto.MESSAGE, number=2, message="FeedAttribute",)


__all__ = tuple(sorted(__protobuf__.manifest))
