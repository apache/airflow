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

from google.ads.googleads.v12.common.types import simulation
from google.ads.googleads.v12.enums.types import simulation_modification_method
from google.ads.googleads.v12.enums.types import simulation_type


__protobuf__ = proto.module(
    package="google.ads.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"AdGroupSimulation",},
)


class AdGroupSimulation(proto.Message):
    r"""An ad group simulation. Supported combinations of advertising
    channel type, simulation type and simulation modification method is
    detailed below respectively.

    1. SEARCH - CPC_BID - DEFAULT
    2. SEARCH - CPC_BID - UNIFORM
    3. SEARCH - TARGET_CPA - UNIFORM
    4. SEARCH - TARGET_ROAS - UNIFORM
    5. DISPLAY - CPC_BID - DEFAULT
    6. DISPLAY - CPC_BID - UNIFORM
    7. DISPLAY - TARGET_CPA - UNIFORM

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        resource_name (str):
            Output only. The resource name of the ad group simulation.
            Ad group simulation resource names have the form:

            ``customers/{customer_id}/adGroupSimulations/{ad_group_id}~{type}~{modification_method}~{start_date}~{end_date}``
        ad_group_id (int):
            Output only. Ad group id of the simulation.

            This field is a member of `oneof`_ ``_ad_group_id``.
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
            is based, in YYYY-MM-DD format

            This field is a member of `oneof`_ ``_end_date``.
        cpc_bid_point_list (google.ads.googleads.v12.common.types.CpcBidSimulationPointList):
            Output only. Simulation points if the simulation type is
            CPC_BID.

            This field is a member of `oneof`_ ``point_list``.
        cpv_bid_point_list (google.ads.googleads.v12.common.types.CpvBidSimulationPointList):
            Output only. Simulation points if the simulation type is
            CPV_BID.

            This field is a member of `oneof`_ ``point_list``.
        target_cpa_point_list (google.ads.googleads.v12.common.types.TargetCpaSimulationPointList):
            Output only. Simulation points if the simulation type is
            TARGET_CPA.

            This field is a member of `oneof`_ ``point_list``.
        target_roas_point_list (google.ads.googleads.v12.common.types.TargetRoasSimulationPointList):
            Output only. Simulation points if the simulation type is
            TARGET_ROAS.

            This field is a member of `oneof`_ ``point_list``.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    ad_group_id = proto.Field(proto.INT64, number=12, optional=True,)
    type_ = proto.Field(
        proto.ENUM,
        number=3,
        enum=simulation_type.SimulationTypeEnum.SimulationType,
    )
    modification_method = proto.Field(
        proto.ENUM,
        number=4,
        enum=simulation_modification_method.SimulationModificationMethodEnum.SimulationModificationMethod,
    )
    start_date = proto.Field(proto.STRING, number=13, optional=True,)
    end_date = proto.Field(proto.STRING, number=14, optional=True,)
    cpc_bid_point_list = proto.Field(
        proto.MESSAGE,
        number=8,
        oneof="point_list",
        message=simulation.CpcBidSimulationPointList,
    )
    cpv_bid_point_list = proto.Field(
        proto.MESSAGE,
        number=10,
        oneof="point_list",
        message=simulation.CpvBidSimulationPointList,
    )
    target_cpa_point_list = proto.Field(
        proto.MESSAGE,
        number=9,
        oneof="point_list",
        message=simulation.TargetCpaSimulationPointList,
    )
    target_roas_point_list = proto.Field(
        proto.MESSAGE,
        number=11,
        oneof="point_list",
        message=simulation.TargetRoasSimulationPointList,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
