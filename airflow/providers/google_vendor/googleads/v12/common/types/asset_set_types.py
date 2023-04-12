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

from airflow.providers.google_vendor.googleads.v12.enums.types import chain_relationship_type
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    location_ownership_type as gage_location_ownership_type,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import location_string_filter_type


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.common",
    marshal="google.ads.googleads.v12",
    manifest={
        "LocationSet",
        "BusinessProfileLocationSet",
        "ChainSet",
        "ChainFilter",
        "MapsLocationSet",
        "MapsLocationInfo",
        "BusinessProfileLocationGroup",
        "DynamicBusinessProfileLocationGroupFilter",
        "BusinessProfileBusinessNameFilter",
        "ChainLocationGroup",
    },
)


class LocationSet(proto.Message):
    r"""Data related to location set. One of the Google Business
    Profile (previously known as Google My Business) data, Chain
    data, and map location data need to be specified.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        location_ownership_type (google.ads.googleads.v12.enums.types.LocationOwnershipTypeEnum.LocationOwnershipType):
            Required. Immutable. Location Ownership Type
            (owned location or affiliate location).
        business_profile_location_set (google.ads.googleads.v12.common.types.BusinessProfileLocationSet):
            Data used to configure a location set
            populated from Google Business Profile
            locations.

            This field is a member of `oneof`_ ``source``.
        chain_location_set (google.ads.googleads.v12.common.types.ChainSet):
            Data used to configure a location on chain
            set populated with the specified chains.

            This field is a member of `oneof`_ ``source``.
        maps_location_set (google.ads.googleads.v12.common.types.MapsLocationSet):
            Only set if locations are synced based on
            selected maps locations

            This field is a member of `oneof`_ ``source``.
    """

    location_ownership_type = proto.Field(
        proto.ENUM,
        number=3,
        enum=gage_location_ownership_type.LocationOwnershipTypeEnum.LocationOwnershipType,
    )
    business_profile_location_set = proto.Field(
        proto.MESSAGE,
        number=1,
        oneof="source",
        message="BusinessProfileLocationSet",
    )
    chain_location_set = proto.Field(
        proto.MESSAGE, number=2, oneof="source", message="ChainSet",
    )
    maps_location_set = proto.Field(
        proto.MESSAGE, number=5, oneof="source", message="MapsLocationSet",
    )


class BusinessProfileLocationSet(proto.Message):
    r"""Data used to configure a location set populated from Google
    Business Profile locations.
    Different types of filters are AND'ed together, if they are
    specified.

    Attributes:
        http_authorization_token (str):
            Required. Immutable. The HTTP authorization
            token used to obtain authorization.
        email_address (str):
            Required. Immutable. Email address of a
            Google Business Profile account or email address
            of a manager of the Google Business Profile
            account.
        business_name_filter (str):
            Used to filter Google Business Profile
            listings by business name. If businessNameFilter
            is set, only listings with a matching business
            name are candidates to be sync'd into Assets.
        label_filters (Sequence[str]):
            Used to filter Google Business Profile
            listings by labels. If entries exist in
            labelFilters, only listings that have any of the
            labels set are candidates to be synchronized
            into Assets. If no entries exist in
            labelFilters, then all listings are candidates
            for syncing. Label filters are OR'ed together.
        listing_id_filters (Sequence[int]):
            Used to filter Google Business Profile
            listings by listing id. If entries exist in
            listingIdFilters, only listings specified by the
            filters are candidates to be synchronized into
            Assets. If no entries exist in listingIdFilters,
            then all listings are candidates for syncing.
            Listing ID filters are OR'ed together.
        business_account_id (str):
            Immutable. The account ID of the managed
            business whose locations are to be used. If this
            field is not set, then all businesses accessible
            by the user (specified by the emailAddress) are
            used.
    """

    http_authorization_token = proto.Field(proto.STRING, number=1,)
    email_address = proto.Field(proto.STRING, number=2,)
    business_name_filter = proto.Field(proto.STRING, number=3,)
    label_filters = proto.RepeatedField(proto.STRING, number=4,)
    listing_id_filters = proto.RepeatedField(proto.INT64, number=5,)
    business_account_id = proto.Field(proto.STRING, number=6,)


class ChainSet(proto.Message):
    r"""Data used to configure a location set populated with the
    specified chains.

    Attributes:
        relationship_type (google.ads.googleads.v12.enums.types.ChainRelationshipTypeEnum.ChainRelationshipType):
            Required. Immutable. Relationship type the
            specified chains have with this advertiser.
        chains (Sequence[google.ads.googleads.v12.common.types.ChainFilter]):
            Required. A list of chain level filters, all
            filters are OR'ed together.
    """

    relationship_type = proto.Field(
        proto.ENUM,
        number=1,
        enum=chain_relationship_type.ChainRelationshipTypeEnum.ChainRelationshipType,
    )
    chains = proto.RepeatedField(
        proto.MESSAGE, number=2, message="ChainFilter",
    )


class ChainFilter(proto.Message):
    r"""One chain level filter on location in a feed item set.
    The filtering logic among all the fields is AND.

    Attributes:
        chain_id (int):
            Required. Used to filter chain locations by
            chain id. Only chain locations that belong to
            the specified chain will be in the asset set.
        location_attributes (Sequence[str]):
            Used to filter chain locations by location
            attributes. Only chain locations that belong to
            all of the specified attribute(s) will be in the
            asset set. If this field is empty, it means no
            filtering on this field.
    """

    chain_id = proto.Field(proto.INT64, number=1,)
    location_attributes = proto.RepeatedField(proto.STRING, number=2,)


class MapsLocationSet(proto.Message):
    r"""Wrapper for multiple maps location sync data

    Attributes:
        maps_locations (Sequence[google.ads.googleads.v12.common.types.MapsLocationInfo]):
            Required. A list of maps location info that
            user manually synced in.
    """

    maps_locations = proto.RepeatedField(
        proto.MESSAGE, number=1, message="MapsLocationInfo",
    )


class MapsLocationInfo(proto.Message):
    r"""Wrapper for place ids

    Attributes:
        place_id (str):
            Place ID of the Maps location.
    """

    place_id = proto.Field(proto.STRING, number=1,)


class BusinessProfileLocationGroup(proto.Message):
    r"""Information about a Business Profile dynamic location group. Only
    applicable if the sync level AssetSet's type is LOCATION_SYNC and
    sync source is Business Profile.

    Attributes:
        dynamic_business_profile_location_group_filter (google.ads.googleads.v12.common.types.DynamicBusinessProfileLocationGroupFilter):
            Filter for dynamic Business Profile location
            sets.
    """

    dynamic_business_profile_location_group_filter = proto.Field(
        proto.MESSAGE,
        number=1,
        message="DynamicBusinessProfileLocationGroupFilter",
    )


class DynamicBusinessProfileLocationGroupFilter(proto.Message):
    r"""Represents a filter on Business Profile locations in an asset
    set. If multiple filters are provided, they are AND'ed together.

    Attributes:
        label_filters (Sequence[str]):
            Used to filter Business Profile locations by
            label. Only locations that have any of the
            listed labels will be in the asset set. Label
            filters are OR'ed together.
        business_name_filter (google.ads.googleads.v12.common.types.BusinessProfileBusinessNameFilter):
            Used to filter Business Profile locations by
            business name.

            This field is a member of `oneof`_ ``_business_name_filter``.
        listing_id_filters (Sequence[int]):
            Used to filter Business Profile locations by
            listing ids.
    """

    label_filters = proto.RepeatedField(proto.STRING, number=1,)
    business_name_filter = proto.Field(
        proto.MESSAGE,
        number=2,
        optional=True,
        message="BusinessProfileBusinessNameFilter",
    )
    listing_id_filters = proto.RepeatedField(proto.INT64, number=3,)


class BusinessProfileBusinessNameFilter(proto.Message):
    r"""Business Profile location group business name filter.

    Attributes:
        business_name (str):
            Business name string to use for filtering.
        filter_type (google.ads.googleads.v12.enums.types.LocationStringFilterTypeEnum.LocationStringFilterType):
            The type of string matching to use when filtering with
            business_name.
    """

    business_name = proto.Field(proto.STRING, number=1,)
    filter_type = proto.Field(
        proto.ENUM,
        number=2,
        enum=location_string_filter_type.LocationStringFilterTypeEnum.LocationStringFilterType,
    )


class ChainLocationGroup(proto.Message):
    r"""Represents information about a Chain dynamic location group. Only
    applicable if the sync level AssetSet's type is LOCATION_SYNC and
    sync source is chain.

    Attributes:
        dynamic_chain_location_group_filters (Sequence[google.ads.googleads.v12.common.types.ChainFilter]):
            Used to filter chain locations by chain ids.
            Only Locations that belong to the specified
            chain(s) will be in the asset set.
    """

    dynamic_chain_location_group_filters = proto.RepeatedField(
        proto.MESSAGE, number=1, message="ChainFilter",
    )


__all__ = tuple(sorted(__protobuf__.manifest))
