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
    manifest={"CampaignCriterionSimulation",},
)


class CampaignCriterionSimulation(proto.Message):
    r"""A campaign criterion simulation. Supported combinations of
    advertising channel type, criterion ids, simulation type and
    simulation modification method is detailed below respectively.

    1. SEARCH - 30000,30001,30002 - BID_MODIFIER - UNIFORM
    2. DISPLAY - 30001 - BID_MODIFIER - UNIFORM


    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        resource_name (str):
            Output only. The resource name of the campaign criterion
            simulation. Campaign criterion simulation resource names
            have the form:

            ``customers/{customer_id}/campaignCriterionSimulations/{campaign_id}~{criterion_id}~{type}~{modification_method}~{start_date}~{end_date}``
        campaign_id (int):
            Output only. Campaign ID of the simulation.

            This field is a member of `oneof`_ ``_campaign_id``.
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
        bid_modifier_point_list (google.ads.googleads.v12.common.types.BidModifierSimulationPointList):
            Output only. Simulation points if the simulation type is
            BID_MODIFIER.

            This field is a member of `oneof`_ ``point_list``.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    campaign_id = proto.Field(proto.INT64, number=9, optional=True,)
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
    bid_modifier_point_list = proto.Field(
        proto.MESSAGE,
        number=8,
        oneof="point_list",
        message=simulation.BidModifierSimulationPointList,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
