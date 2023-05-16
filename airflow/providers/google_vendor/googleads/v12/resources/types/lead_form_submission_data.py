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

from airflow.providers.google_vendor.googleads.v12.enums.types import lead_form_field_user_input_type


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={
        "LeadFormSubmissionData",
        "LeadFormSubmissionField",
        "CustomLeadFormSubmissionField",
    },
)


class LeadFormSubmissionData(proto.Message):
    r"""Data from lead form submissions.

    Attributes:
        resource_name (str):
            Output only. The resource name of the lead form submission
            data. Lead form submission data resource names have the
            form:

            ``customers/{customer_id}/leadFormSubmissionData/{lead_form_submission_data_id}``
        id (str):
            Output only. ID of this lead form submission.
        asset (str):
            Output only. Asset associated with the
            submitted lead form.
        campaign (str):
            Output only. Campaign associated with the
            submitted lead form.
        lead_form_submission_fields (Sequence[google.ads.googleads.v12.resources.types.LeadFormSubmissionField]):
            Output only. Submission data associated with
            a lead form.
        custom_lead_form_submission_fields (Sequence[google.ads.googleads.v12.resources.types.CustomLeadFormSubmissionField]):
            Output only. Submission data associated with
            a custom lead form.
        ad_group (str):
            Output only. AdGroup associated with the
            submitted lead form.
        ad_group_ad (str):
            Output only. AdGroupAd associated with the
            submitted lead form.
        gclid (str):
            Output only. Google Click Id associated with
            the submissed lead form.
        submission_date_time (str):
            Output only. The date and time at which the lead form was
            submitted. The format is "yyyy-mm-dd hh:mm:ss+|-hh:mm", for
            example, "2019-01-01 12:32:45-08:00".
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    id = proto.Field(proto.STRING, number=2,)
    asset = proto.Field(proto.STRING, number=3,)
    campaign = proto.Field(proto.STRING, number=4,)
    lead_form_submission_fields = proto.RepeatedField(
        proto.MESSAGE, number=5, message="LeadFormSubmissionField",
    )
    custom_lead_form_submission_fields = proto.RepeatedField(
        proto.MESSAGE, number=10, message="CustomLeadFormSubmissionField",
    )
    ad_group = proto.Field(proto.STRING, number=6,)
    ad_group_ad = proto.Field(proto.STRING, number=7,)
    gclid = proto.Field(proto.STRING, number=8,)
    submission_date_time = proto.Field(proto.STRING, number=9,)


class LeadFormSubmissionField(proto.Message):
    r"""Fields in the submitted lead form.

    Attributes:
        field_type (google.ads.googleads.v12.enums.types.LeadFormFieldUserInputTypeEnum.LeadFormFieldUserInputType):
            Output only. Field type for lead form fields.
        field_value (str):
            Output only. Field value for lead form
            fields.
    """

    field_type = proto.Field(
        proto.ENUM,
        number=1,
        enum=lead_form_field_user_input_type.LeadFormFieldUserInputTypeEnum.LeadFormFieldUserInputType,
    )
    field_value = proto.Field(proto.STRING, number=2,)


class CustomLeadFormSubmissionField(proto.Message):
    r"""Fields in the submitted custom question

    Attributes:
        question_text (str):
            Output only. Question text for custom
            question, maximum number of characters is 300.
        field_value (str):
            Output only. Field value for custom question
            response, maximum number of characters is 70.
    """

    question_text = proto.Field(proto.STRING, number=1,)
    field_value = proto.Field(proto.STRING, number=2,)


__all__ = tuple(sorted(__protobuf__.manifest))
