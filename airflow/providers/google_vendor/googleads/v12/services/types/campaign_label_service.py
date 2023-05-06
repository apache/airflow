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

from airflow.providers.google_vendor.googleads.v12.resources.types import campaign_label
from google.rpc import status_pb2  # type: ignore


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.services",
    marshal="google.ads.googleads.v12",
    manifest={
        "MutateCampaignLabelsRequest",
        "CampaignLabelOperation",
        "MutateCampaignLabelsResponse",
        "MutateCampaignLabelResult",
    },
)


class MutateCampaignLabelsRequest(proto.Message):
    r"""Request message for
    [CampaignLabelService.MutateCampaignLabels][google.ads.googleads.v12.services.CampaignLabelService.MutateCampaignLabels].

    Attributes:
        customer_id (str):
            Required. ID of the customer whose
            campaign-label relationships are being modified.
        operations (Sequence[google.ads.googleads.v12.services.types.CampaignLabelOperation]):
            Required. The list of operations to perform
            on campaign-label relationships.
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
        proto.MESSAGE, number=2, message="CampaignLabelOperation",
    )
    partial_failure = proto.Field(proto.BOOL, number=3,)
    validate_only = proto.Field(proto.BOOL, number=4,)


class CampaignLabelOperation(proto.Message):
    r"""A single operation (create, remove) on a campaign-label
    relationship.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        create (google.ads.googleads.v12.resources.types.CampaignLabel):
            Create operation: No resource name is
            expected for the new campaign-label
            relationship.

            This field is a member of `oneof`_ ``operation``.
        remove (str):
            Remove operation: A resource name for the campaign-label
            relationship being removed, in this format:

            ``customers/{customer_id}/campaignLabels/{campaign_id}~{label_id}``

            This field is a member of `oneof`_ ``operation``.
    """

    create = proto.Field(
        proto.MESSAGE,
        number=1,
        oneof="operation",
        message=campaign_label.CampaignLabel,
    )
    remove = proto.Field(proto.STRING, number=2, oneof="operation",)


class MutateCampaignLabelsResponse(proto.Message):
    r"""Response message for a campaign labels mutate.

    Attributes:
        partial_failure_error (google.rpc.status_pb2.Status):
            Errors that pertain to operation failures in the partial
            failure mode. Returned only when partial_failure = true and
            all errors occur inside the operations. If any errors occur
            outside the operations (for example, auth errors), we return
            an RPC level error.
        results (Sequence[google.ads.googleads.v12.services.types.MutateCampaignLabelResult]):
            All results for the mutate.
    """

    partial_failure_error = proto.Field(
        proto.MESSAGE, number=3, message=status_pb2.Status,
    )
    results = proto.RepeatedField(
        proto.MESSAGE, number=2, message="MutateCampaignLabelResult",
    )


class MutateCampaignLabelResult(proto.Message):
    r"""The result for a campaign label mutate.

    Attributes:
        resource_name (str):
            Returned for successful operations.
    """

    resource_name = proto.Field(proto.STRING, number=1,)


__all__ = tuple(sorted(__protobuf__.manifest))
