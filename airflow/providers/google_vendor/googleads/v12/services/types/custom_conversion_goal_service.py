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
    response_content_type as gage_response_content_type,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    custom_conversion_goal as gagr_custom_conversion_goal,
)
from google.protobuf import field_mask_pb2  # type: ignore


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.services",
    marshal="google.ads.googleads.v12",
    manifest={
        "MutateCustomConversionGoalsRequest",
        "CustomConversionGoalOperation",
        "MutateCustomConversionGoalsResponse",
        "MutateCustomConversionGoalResult",
    },
)


class MutateCustomConversionGoalsRequest(proto.Message):
    r"""Request message for
    [CustomConversionGoalService.MutateCustomConversionGoals][google.ads.googleads.v12.services.CustomConversionGoalService.MutateCustomConversionGoals].

    Attributes:
        customer_id (str):
            Required. The ID of the customer whose custom
            conversion goals are being modified.
        operations (Sequence[google.ads.googleads.v12.services.types.CustomConversionGoalOperation]):
            Required. The list of operations to perform
            on individual custom conversion goal.
        validate_only (bool):
            If true, the request is validated but not
            executed. Only errors are returned, not results.
        response_content_type (google.ads.googleads.v12.enums.types.ResponseContentTypeEnum.ResponseContentType):
            The response content type setting. Determines
            whether the mutable resource or just the
            resource name should be returned post mutation.
    """

    customer_id = proto.Field(proto.STRING, number=1,)
    operations = proto.RepeatedField(
        proto.MESSAGE, number=2, message="CustomConversionGoalOperation",
    )
    validate_only = proto.Field(proto.BOOL, number=3,)
    response_content_type = proto.Field(
        proto.ENUM,
        number=4,
        enum=gage_response_content_type.ResponseContentTypeEnum.ResponseContentType,
    )


class CustomConversionGoalOperation(proto.Message):
    r"""A single operation (create, remove) on a custom conversion
    goal.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        update_mask (google.protobuf.field_mask_pb2.FieldMask):
            FieldMask that determines which resource
            fields are modified in an update.
        create (google.ads.googleads.v12.resources.types.CustomConversionGoal):
            Create operation: No resource name is
            expected for the new custom conversion goal

            This field is a member of `oneof`_ ``operation``.
        update (google.ads.googleads.v12.resources.types.CustomConversionGoal):
            Update operation: The custom conversion goal
            is expected to have a valid resource name.

            This field is a member of `oneof`_ ``operation``.
        remove (str):
            Remove operation: A resource name for the removed custom
            conversion goal is expected, in this format:

            'customers/{customer_id}/conversionActions/{ConversionGoal.custom_goal_config.conversion_type_ids}'

            This field is a member of `oneof`_ ``operation``.
    """

    update_mask = proto.Field(
        proto.MESSAGE, number=4, message=field_mask_pb2.FieldMask,
    )
    create = proto.Field(
        proto.MESSAGE,
        number=1,
        oneof="operation",
        message=gagr_custom_conversion_goal.CustomConversionGoal,
    )
    update = proto.Field(
        proto.MESSAGE,
        number=2,
        oneof="operation",
        message=gagr_custom_conversion_goal.CustomConversionGoal,
    )
    remove = proto.Field(proto.STRING, number=3, oneof="operation",)


class MutateCustomConversionGoalsResponse(proto.Message):
    r"""Response message for a custom conversion goal mutate.

    Attributes:
        results (Sequence[google.ads.googleads.v12.services.types.MutateCustomConversionGoalResult]):
            All results for the mutate.
    """

    results = proto.RepeatedField(
        proto.MESSAGE, number=1, message="MutateCustomConversionGoalResult",
    )


class MutateCustomConversionGoalResult(proto.Message):
    r"""The result for the custom conversion goal mutate.

    Attributes:
        resource_name (str):
            Returned for successful operations.
        custom_conversion_goal (google.ads.googleads.v12.resources.types.CustomConversionGoal):
            The mutated CustomConversionGoal with only mutable fields
            after mutate. The field will only be returned when
            response_content_type is set to "MUTABLE_RESOURCE".
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    custom_conversion_goal = proto.Field(
        proto.MESSAGE,
        number=2,
        message=gagr_custom_conversion_goal.CustomConversionGoal,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
