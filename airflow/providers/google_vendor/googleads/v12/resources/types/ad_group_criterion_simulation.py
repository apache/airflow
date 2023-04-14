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

from airflow.providers.google_vendor.googleads.v12.common.types import simulation
from airflow.providers.google_vendor.googleads.v12.enums.types import simulation_modification_method
from airflow.providers.google_vendor.googleads.v12.enums.types import simulation_type


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"AdGroupCriterionSimulation",},
)


class AdGroupCriterionSimulation(proto.Message):
    r"""An ad group criterion simulation. Supported combinations of
    advertising channel type, criterion type, simulation type, and
    simulation modification method are detailed below respectively.
    Hotel AdGroupCriterion simulation operations starting in V5.

    1. DISPLAY - KEYWORD - CPC_BID - UNIFORM
    2. SEARCH - KEYWORD - CPC_BID - UNIFORM
    3. SHOPPING - LISTING_GROUP - CPC_BID - UNIFORM
    4. HOTEL - LISTING_GROUP - CPC_BID - UNIFORM
    5. HOTEL - LISTING_GROUP - PERCENT_CPC_BID - UNIFORM

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        resource_name (str):
            Output only. The resource name of the ad group criterion
            simulation. Ad group criterion simulation resource names
            have the form:

            ``customers/{customer_id}/adGroupCriterionSimulations/{ad_group_id}~{criterion_id}~{type}~{modification_method}~{start_date}~{end_date}``
        ad_group_id (int):
            Output only. AdGroup ID of the simulation.

            This field is a member of `oneof`_ ``_ad_group_id``.
        criterion_id (int):
            Output only. Criterion ID of the simulation.

            This field is a member of `oneof`_ ``_criterion_id``.
        type_ (google.ads.googleads.v12.enums.types.SimulationTypeEnum.SimulationType):
            Output only. The field that the simulation
            modifies.
        modification_method (google.ads.googleads.v12.enums.types.SimulationModificationMethodEnum.SimulationModificationMethod):
            Output only. How the simulation modifies the
            field.
        start_date (str):
            Output only. First day on which the
            simulation is based, in YYYY-MM-DD format.

            This field is a member of `oneof`_ ``_start_date``.
        end_date (str):
            Output only. Last day on which the simulation
            is based, in YYYY-MM-DD format.

            This field is a member of `oneof`_ ``_end_date``.
        cpc_bid_point_list (google.ads.googleads.v12.common.types.CpcBidSimulationPointList):
            Output only. Simulation points if the simulation type is
            CPC_BID.

            This field is a member of `oneof`_ ``point_list``.
        percent_cpc_bid_point_list (google.ads.googleads.v12.common.types.PercentCpcBidSimulationPointList):
            Output only. Simulation points if the simulation type is
            PERCENT_CPC_BID.

            This field is a member of `oneof`_ ``point_list``.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    ad_group_id = proto.Field(proto.INT64, number=9, optional=True,)
    criterion_id = proto.Field(proto.INT64, number=10, optional=True,)
    type_ = proto.Field(
        proto.ENUM,
        number=4,
        enum=simulation_type.SimulationTypeEnum.SimulationType,
    )
    modification_method = proto.Field(
        proto.ENUM,
        number=5,
        enum=simulation_modification_method.SimulationModificationMethodEnum.SimulationModificationMethod,
    )
    start_date = proto.Field(proto.STRING, number=11, optional=True,)
    end_date = proto.Field(proto.STRING, number=12, optional=True,)
    cpc_bid_point_list = proto.Field(
        proto.MESSAGE,
        number=8,
        oneof="point_list",
        message=simulation.CpcBidSimulationPointList,
    )
    percent_cpc_bid_point_list = proto.Field(
        proto.MESSAGE,
        number=13,
        oneof="point_list",
        message=simulation.PercentCpcBidSimulationPointList,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
