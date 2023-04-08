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
    manifest={"BiddingStrategySimulation",},
)


class BiddingStrategySimulation(proto.Message):
    r"""A bidding strategy simulation. Supported combinations of simulation
    type and simulation modification method are detailed below
    respectively.

    1. TARGET_CPA - UNIFORM
    2. TARGET_ROAS - UNIFORM

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        resource_name (str):
            Output only. The resource name of the bidding strategy
            simulation. Bidding strategy simulation resource names have
            the form:

            ``customers/{customer_id}/biddingStrategySimulations/{bidding_strategy_id}~{type}~{modification_method}~{start_date}~{end_date}``
        bidding_strategy_id (int):
            Output only. Bidding strategy shared set id
            of the simulation.
        type_ (google.ads.googleads.v12.enums.types.SimulationTypeEnum.SimulationType):
            Output only. The field that the simulation
            modifies.
        modification_method (google.ads.googleads.v12.enums.types.SimulationModificationMethodEnum.SimulationModificationMethod):
            Output only. How the simulation modifies the
            field.
        start_date (str):
            Output only. First day on which the
            simulation is based, in YYYY-MM-DD format.
        end_date (str):
            Output only. Last day on which the simulation
            is based, in YYYY-MM-DD format
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
    bidding_strategy_id = proto.Field(proto.INT64, number=2,)
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
    start_date = proto.Field(proto.STRING, number=5,)
    end_date = proto.Field(proto.STRING, number=6,)
    target_cpa_point_list = proto.Field(
        proto.MESSAGE,
        number=7,
        oneof="point_list",
        message=simulation.TargetCpaSimulationPointList,
    )
    target_roas_point_list = proto.Field(
        proto.MESSAGE,
        number=8,
        oneof="point_list",
        message=simulation.TargetRoasSimulationPointList,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
