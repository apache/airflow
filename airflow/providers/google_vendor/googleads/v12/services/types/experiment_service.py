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

from airflow.providers.google_vendor.googleads.v12.resources.types import (
    experiment as gagr_experiment,
)
from google.protobuf import field_mask_pb2  # type: ignore
from google.rpc import status_pb2  # type: ignore


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.services",
    marshal="google.ads.googleads.v12",
    manifest={
        "MutateExperimentsRequest",
        "ExperimentOperation",
        "MutateExperimentsResponse",
        "MutateExperimentResult",
        "EndExperimentRequest",
        "ListExperimentAsyncErrorsRequest",
        "ListExperimentAsyncErrorsResponse",
        "GraduateExperimentRequest",
        "CampaignBudgetMapping",
        "ScheduleExperimentRequest",
        "ScheduleExperimentMetadata",
        "PromoteExperimentRequest",
        "PromoteExperimentMetadata",
    },
)


class MutateExperimentsRequest(proto.Message):
    r"""Request message for
    [ExperimentService.MutateExperiments][google.ads.googleads.v12.services.ExperimentService.MutateExperiments].

    Attributes:
        customer_id (str):
            Required. The ID of the customer whose
            experiments are being modified.
        operations (Sequence[google.ads.googleads.v12.services.types.ExperimentOperation]):
            Required. The list of operations to perform
            on individual experiments.
        partial_failure (bool):
            If true, successful operations will be
            carried out and invalid operations will return
            errors. If false, all operations will be carried
            out in one transaction if and only if they are
            all valid. Default is false.
        validate_only (bool):
            If true, the request is validated but not
            executed. Only errors are returned, not results.
    """

    customer_id = proto.Field(proto.STRING, number=1,)
    operations = proto.RepeatedField(
        proto.MESSAGE, number=2, message="ExperimentOperation",
    )
    partial_failure = proto.Field(proto.BOOL, number=3,)
    validate_only = proto.Field(proto.BOOL, number=4,)


class ExperimentOperation(proto.Message):
    r"""A single operation on an experiment.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        update_mask (google.protobuf.field_mask_pb2.FieldMask):
            FieldMask that determines which resource
            fields are modified in an update.
        create (google.ads.googleads.v12.resources.types.Experiment):
            Create operation

            This field is a member of `oneof`_ ``operation``.
        update (google.ads.googleads.v12.resources.types.Experiment):
            Update operation: The experiment is expected
            to have a valid resource name.

            This field is a member of `oneof`_ ``operation``.
        remove (str):
            Remove operation: The experiment is expected to have a valid
            resource name, in this format:

            ``customers/{customer_id}/experiments/{campaign_experiment_id}``

            This field is a member of `oneof`_ ``operation``.
    """

    update_mask = proto.Field(
        proto.MESSAGE, number=4, message=field_mask_pb2.FieldMask,
    )
    create = proto.Field(
        proto.MESSAGE,
        number=1,
        oneof="operation",
        message=gagr_experiment.Experiment,
    )
    update = proto.Field(
        proto.MESSAGE,
        number=2,
        oneof="operation",
        message=gagr_experiment.Experiment,
    )
    remove = proto.Field(proto.STRING, number=3, oneof="operation",)


class MutateExperimentsResponse(proto.Message):
    r"""Response message for experiment mutate.

    Attributes:
        partial_failure_error (google.rpc.status_pb2.Status):
            Errors that pertain to operation failures in the partial
            failure mode. Returned only when partial_failure = true and
            all errors occur inside the operations. If any errors occur
            outside the operations (for example, auth errors), we return
            an RPC level error.
        results (Sequence[google.ads.googleads.v12.services.types.MutateExperimentResult]):
            All results for the mutate.
    """

    partial_failure_error = proto.Field(
        proto.MESSAGE, number=1, message=status_pb2.Status,
    )
    results = proto.RepeatedField(
        proto.MESSAGE, number=2, message="MutateExperimentResult",
    )


class MutateExperimentResult(proto.Message):
    r"""The result for the campaign experiment mutate.

    Attributes:
        resource_name (str):
            Returned for successful operations.
    """

    resource_name = proto.Field(proto.STRING, number=1,)


class EndExperimentRequest(proto.Message):
    r"""Request message for
    [ExperimentService.EndExperiment][google.ads.googleads.v12.services.ExperimentService.EndExperiment].

    Attributes:
        experiment (str):
            Required. The resource name of the campaign
            experiment to end.
        validate_only (bool):
            If true, the request is validated but not
            executed. Only errors are returned, not results.
    """

    experiment = proto.Field(proto.STRING, number=1,)
    validate_only = proto.Field(proto.BOOL, number=2,)


class ListExperimentAsyncErrorsRequest(proto.Message):
    r"""Request message for
    [ExperimentService.ListExperimentAsyncErrors][google.ads.googleads.v12.services.ExperimentService.ListExperimentAsyncErrors].

    Attributes:
        resource_name (str):
            Required. The name of the experiment from
            which to retrieve the async errors.
        page_token (str):
            Token of the page to retrieve. If not specified, the first
            page of results will be returned. Use the value obtained
            from ``next_page_token`` in the previous response in order
            to request the next page of results.
        page_size (int):
            Number of elements to retrieve in a single
            page. When a page request is too large, the
            server may decide to further limit the number of
            returned resources. The maximum page size is
            1000.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    page_token = proto.Field(proto.STRING, number=2,)
    page_size = proto.Field(proto.INT32, number=3,)


class ListExperimentAsyncErrorsResponse(proto.Message):
    r"""Response message for
    [ExperimentService.ListExperimentAsyncErrors][google.ads.googleads.v12.services.ExperimentService.ListExperimentAsyncErrors].

    Attributes:
        errors (Sequence[google.rpc.status_pb2.Status]):
            details of the errors when performing the
            asynchronous operation.
        next_page_token (str):
            Pagination token used to retrieve the next page of results.
            Pass the content of this string as the ``page_token``
            attribute of the next request. ``next_page_token`` is not
            returned for the last page.
    """

    @property
    def raw_page(self):
        return self

    errors = proto.RepeatedField(
        proto.MESSAGE, number=1, message=status_pb2.Status,
    )
    next_page_token = proto.Field(proto.STRING, number=2,)


class GraduateExperimentRequest(proto.Message):
    r"""Request message for
    [ExperimentService.GraduateExperiment][google.ads.googleads.v12.services.ExperimentService.GraduateExperiment].

    Attributes:
        experiment (str):
            Required. The experiment to be graduated.
        campaign_budget_mappings (Sequence[google.ads.googleads.v12.services.types.CampaignBudgetMapping]):
            Required. List of campaign budget mappings
            for graduation. Each campaign that appears here
            will graduate, and will be assigned a new budget
            that is paired with it in the mapping. The
            maximum size is one.
        validate_only (bool):
            If true, the request is validated but not
            executed. Only errors are returned, not results.
    """

    experiment = proto.Field(proto.STRING, number=1,)
    campaign_budget_mappings = proto.RepeatedField(
        proto.MESSAGE, number=2, message="CampaignBudgetMapping",
    )
    validate_only = proto.Field(proto.BOOL, number=3,)


class CampaignBudgetMapping(proto.Message):
    r"""The mapping of experiment campaign and budget to be
    graduated.

    Attributes:
        experiment_campaign (str):
            Required. The experiment campaign to
            graduate.
        campaign_budget (str):
            Required. The budget that should be attached
            to the graduating experiment campaign.
    """

    experiment_campaign = proto.Field(proto.STRING, number=1,)
    campaign_budget = proto.Field(proto.STRING, number=2,)


class ScheduleExperimentRequest(proto.Message):
    r"""Request message for
    [ExperimentService.ScheduleExperiment][google.ads.googleads.v12.services.ExperimentService.ScheduleExperiment].

    Attributes:
        resource_name (str):
            Required. The scheduled experiment.
        validate_only (bool):
            If true, the request is validated but not
            executed. Only errors are returned, not results.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    validate_only = proto.Field(proto.BOOL, number=2,)


class ScheduleExperimentMetadata(proto.Message):
    r"""The metadata of the scheduled experiment.

    Attributes:
        experiment (str):
            Required. The scheduled experiment.
    """

    experiment = proto.Field(proto.STRING, number=1,)


class PromoteExperimentRequest(proto.Message):
    r"""Request message for
    [ExperimentService.PromoteExperiment][google.ads.googleads.v12.services.ExperimentService.PromoteExperiment].

    Attributes:
        resource_name (str):
            Required. The resource name of the experiment
            to promote.
        validate_only (bool):
            If true, the request is validated but not
            executed. Only errors are returned, not results.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    validate_only = proto.Field(proto.BOOL, number=2,)


class PromoteExperimentMetadata(proto.Message):
    r"""The metadata of the promoted experiment.

    Attributes:
        experiment (str):
            Required. The promoted experiment.
    """

    experiment = proto.Field(proto.STRING, number=1,)


__all__ = tuple(sorted(__protobuf__.manifest))
