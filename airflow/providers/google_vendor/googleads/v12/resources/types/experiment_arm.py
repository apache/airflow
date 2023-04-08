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
    manifest={"ExperimentArm",},
)


class ExperimentArm(proto.Message):
    r"""A Google ads experiment for users to experiment changes on
    multiple campaigns, compare the performance, and apply the
    effective changes.

    Attributes:
        resource_name (str):
            Immutable. The resource name of the experiment arm.
            Experiment arm resource names have the form:

            ``customers/{customer_id}/experimentArms/{TrialArm.trial_id}~{TrialArm.trial_arm_id}``
        experiment (str):
            Immutable. The experiment to which the
            ExperimentArm belongs.
        name (str):
            Required. The name of the experiment arm. It
            must have a minimum length of 1 and maximum
            length of 1024. It must be unique under an
            experiment.
        control (bool):
            Whether this arm is a control arm. A control
            arm is the arm against which the other arms are
            compared.
        traffic_split (int):
            Traffic split of the trial arm. The value
            should be between 1 and 100 and must total 100
            between the two trial arms.
        campaigns (Sequence[str]):
            List of campaigns in the trial arm. The max
            length is one.
        in_design_campaigns (Sequence[str]):
            Output only. The in design campaigns in the
            treatment experiment arm.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    experiment = proto.Field(proto.STRING, number=8,)
    name = proto.Field(proto.STRING, number=3,)
    control = proto.Field(proto.BOOL, number=4,)
    traffic_split = proto.Field(proto.INT64, number=5,)
    campaigns = proto.RepeatedField(proto.STRING, number=6,)
    in_design_campaigns = proto.RepeatedField(proto.STRING, number=7,)


__all__ = tuple(sorted(__protobuf__.manifest))
