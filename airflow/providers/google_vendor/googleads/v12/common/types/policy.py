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

from airflow.providers.google_vendor.googleads.v12.enums.types import policy_topic_entry_type
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    policy_topic_evidence_destination_mismatch_url_type,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    policy_topic_evidence_destination_not_working_device,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    policy_topic_evidence_destination_not_working_dns_error_type,
)


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.common",
    marshal="google.ads.googleads.v12",
    manifest={
        "PolicyViolationKey",
        "PolicyValidationParameter",
        "PolicyTopicEntry",
        "PolicyTopicEvidence",
        "PolicyTopicConstraint",
    },
)


class PolicyViolationKey(proto.Message):
    r"""Key of the violation. The key is used for referring to a
    violation when filing an exemption request.

    Attributes:
        policy_name (str):
            Unique ID of the violated policy.

            This field is a member of `oneof`_ ``_policy_name``.
        violating_text (str):
            The text that violates the policy if
            specified. Otherwise, refers to the policy in
            general (for example, when requesting to be
            exempt from the whole policy). If not specified
            for criterion exemptions, the whole policy is
            implied. Must be specified for ad exemptions.

            This field is a member of `oneof`_ ``_violating_text``.
    """

    policy_name = proto.Field(proto.STRING, number=3, optional=True,)
    violating_text = proto.Field(proto.STRING, number=4, optional=True,)


class PolicyValidationParameter(proto.Message):
    r"""Parameter for controlling how policy exemption is done.

    Attributes:
        ignorable_policy_topics (Sequence[str]):
            The list of policy topics that should not
            cause a PolicyFindingError to be reported. This
            field is currently only compatible with Enhanced
            Text Ad. It corresponds to the
            PolicyTopicEntry.topic field.
            Resources violating these policies will be
            saved, but will not be eligible to serve. They
            may begin serving at a later time due to a
            change in policies, re-review of the resource,
            or a change in advertiser certificates.
        exempt_policy_violation_keys (Sequence[google.ads.googleads.v12.common.types.PolicyViolationKey]):
            The list of policy violation keys that should not cause a
            PolicyViolationError to be reported. Not all policy
            violations are exemptable, refer to the is_exemptible field
            in the returned PolicyViolationError.

            Resources violating these polices will be saved, but will
            not be eligible to serve. They may begin serving at a later
            time due to a change in policies, re-review of the resource,
            or a change in advertiser certificates.
    """

    ignorable_policy_topics = proto.RepeatedField(proto.STRING, number=3,)
    exempt_policy_violation_keys = proto.RepeatedField(
        proto.MESSAGE, number=2, message="PolicyViolationKey",
    )


class PolicyTopicEntry(proto.Message):
    r"""Policy finding attached to a resource (for example, alcohol
    policy associated with a site that sells alcohol).

    Each PolicyTopicEntry has a topic that indicates the specific
    ads policy the entry is about and a type to indicate the effect
    that the entry will have on serving. It may optionally have one
    or more evidences that indicate the reason for the finding. It
    may also optionally have one or more constraints that provide
    details about how serving may be restricted.

    Attributes:
        topic (str):
            Policy topic this finding refers to. For example, "ALCOHOL",
            "TRADEMARKS_IN_AD_TEXT", or "DESTINATION_NOT_WORKING". The
            set of possible policy topics is not fixed for a particular
            API version and may change at any time.

            This field is a member of `oneof`_ ``_topic``.
        type_ (google.ads.googleads.v12.enums.types.PolicyTopicEntryTypeEnum.PolicyTopicEntryType):
            Describes the negative or positive effect
            this policy will have on serving.
        evidences (Sequence[google.ads.googleads.v12.common.types.PolicyTopicEvidence]):
            Additional information that explains policy
            finding (for example, the brand name for a
            trademark finding).
        constraints (Sequence[google.ads.googleads.v12.common.types.PolicyTopicConstraint]):
            Indicates how serving of this resource may be
            affected (for example, not serving in a
            country).
    """

    topic = proto.Field(proto.STRING, number=5, optional=True,)
    type_ = proto.Field(
        proto.ENUM,
        number=2,
        enum=policy_topic_entry_type.PolicyTopicEntryTypeEnum.PolicyTopicEntryType,
    )
    evidences = proto.RepeatedField(
        proto.MESSAGE, number=3, message="PolicyTopicEvidence",
    )
    constraints = proto.RepeatedField(
        proto.MESSAGE, number=4, message="PolicyTopicConstraint",
    )


class PolicyTopicEvidence(proto.Message):
    r"""Additional information that explains a policy finding.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        website_list (google.ads.googleads.v12.common.types.PolicyTopicEvidence.WebsiteList):
            List of websites linked with this resource.

            This field is a member of `oneof`_ ``value``.
        text_list (google.ads.googleads.v12.common.types.PolicyTopicEvidence.TextList):
            List of evidence found in the text of a
            resource.

            This field is a member of `oneof`_ ``value``.
        language_code (str):
            The language the resource was detected to be
            written in. This is an IETF language tag such as
            "en-US".

            This field is a member of `oneof`_ ``value``.
        destination_text_list (google.ads.googleads.v12.common.types.PolicyTopicEvidence.DestinationTextList):
            The text in the destination of the resource
            that is causing a policy finding.

            This field is a member of `oneof`_ ``value``.
        destination_mismatch (google.ads.googleads.v12.common.types.PolicyTopicEvidence.DestinationMismatch):
            Mismatch between the destinations of a
            resource's URLs.

            This field is a member of `oneof`_ ``value``.
        destination_not_working (google.ads.googleads.v12.common.types.PolicyTopicEvidence.DestinationNotWorking):
            Details when the destination is returning an
            HTTP error code or isn't functional in all
            locations for commonly used devices.

            This field is a member of `oneof`_ ``value``.
    """

    class TextList(proto.Message):
        r"""A list of fragments of text that violated a policy.

        Attributes:
            texts (Sequence[str]):
                The fragments of text from the resource that
                caused the policy finding.
        """

        texts = proto.RepeatedField(proto.STRING, number=2,)

    class WebsiteList(proto.Message):
        r"""A list of websites that caused a policy finding. Used for
        ONE_WEBSITE_PER_AD_GROUP policy topic, for example. In case there
        are more than five websites, only the top five (those that appear in
        resources the most) will be listed here.

        Attributes:
            websites (Sequence[str]):
                Websites that caused the policy finding.
        """

        websites = proto.RepeatedField(proto.STRING, number=2,)

    class DestinationTextList(proto.Message):
        r"""A list of strings found in a destination page that caused a
        policy finding.

        Attributes:
            destination_texts (Sequence[str]):
                List of text found in the resource's
                destination page.
        """

        destination_texts = proto.RepeatedField(proto.STRING, number=2,)

    class DestinationMismatch(proto.Message):
        r"""Evidence of mismatches between the URLs of a resource.

        Attributes:
            url_types (Sequence[google.ads.googleads.v12.enums.types.PolicyTopicEvidenceDestinationMismatchUrlTypeEnum.PolicyTopicEvidenceDestinationMismatchUrlType]):
                The set of URLs that did not match each
                other.
        """

        url_types = proto.RepeatedField(
            proto.ENUM,
            number=1,
            enum=policy_topic_evidence_destination_mismatch_url_type.PolicyTopicEvidenceDestinationMismatchUrlTypeEnum.PolicyTopicEvidenceDestinationMismatchUrlType,
        )

    class DestinationNotWorking(proto.Message):
        r"""Evidence details when the destination is returning an HTTP
        error code or isn't functional in all locations for commonly
        used devices.

        This message has `oneof`_ fields (mutually exclusive fields).
        For each oneof, at most one member field can be set at the same time.
        Setting any member of the oneof automatically clears all other
        members.

        .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

        Attributes:
            expanded_url (str):
                The full URL that didn't work.

                This field is a member of `oneof`_ ``_expanded_url``.
            device (google.ads.googleads.v12.enums.types.PolicyTopicEvidenceDestinationNotWorkingDeviceEnum.PolicyTopicEvidenceDestinationNotWorkingDevice):
                The type of device that failed to load the
                URL.
            last_checked_date_time (str):
                The time the URL was last checked.
                The format is "YYYY-MM-DD HH:MM:SS".
                Examples: "2018-03-05 09:15:00" or "2018-02-01
                14:34:30".

                This field is a member of `oneof`_ ``_last_checked_date_time``.
            dns_error_type (google.ads.googleads.v12.enums.types.PolicyTopicEvidenceDestinationNotWorkingDnsErrorTypeEnum.PolicyTopicEvidenceDestinationNotWorkingDnsErrorType):
                The type of DNS error.

                This field is a member of `oneof`_ ``reason``.
            http_error_code (int):
                The HTTP error code.

                This field is a member of `oneof`_ ``reason``.
        """

        expanded_url = proto.Field(proto.STRING, number=7, optional=True,)
        device = proto.Field(
            proto.ENUM,
            number=4,
            enum=policy_topic_evidence_destination_not_working_device.PolicyTopicEvidenceDestinationNotWorkingDeviceEnum.PolicyTopicEvidenceDestinationNotWorkingDevice,
        )
        last_checked_date_time = proto.Field(
            proto.STRING, number=8, optional=True,
        )
        dns_error_type = proto.Field(
            proto.ENUM,
            number=1,
            oneof="reason",
            enum=policy_topic_evidence_destination_not_working_dns_error_type.PolicyTopicEvidenceDestinationNotWorkingDnsErrorTypeEnum.PolicyTopicEvidenceDestinationNotWorkingDnsErrorType,
        )
        http_error_code = proto.Field(proto.INT64, number=6, oneof="reason",)

    website_list = proto.Field(
        proto.MESSAGE, number=3, oneof="value", message=WebsiteList,
    )
    text_list = proto.Field(
        proto.MESSAGE, number=4, oneof="value", message=TextList,
    )
    language_code = proto.Field(proto.STRING, number=9, oneof="value",)
    destination_text_list = proto.Field(
        proto.MESSAGE, number=6, oneof="value", message=DestinationTextList,
    )
    destination_mismatch = proto.Field(
        proto.MESSAGE, number=7, oneof="value", message=DestinationMismatch,
    )
    destination_not_working = proto.Field(
        proto.MESSAGE, number=8, oneof="value", message=DestinationNotWorking,
    )


class PolicyTopicConstraint(proto.Message):
    r"""Describes the effect on serving that a policy topic entry
    will have.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        country_constraint_list (google.ads.googleads.v12.common.types.PolicyTopicConstraint.CountryConstraintList):
            Countries where the resource cannot serve.

            This field is a member of `oneof`_ ``value``.
        reseller_constraint (google.ads.googleads.v12.common.types.PolicyTopicConstraint.ResellerConstraint):
            Reseller constraint.

            This field is a member of `oneof`_ ``value``.
        certificate_missing_in_country_list (google.ads.googleads.v12.common.types.PolicyTopicConstraint.CountryConstraintList):
            Countries where a certificate is required for
            serving.

            This field is a member of `oneof`_ ``value``.
        certificate_domain_mismatch_in_country_list (google.ads.googleads.v12.common.types.PolicyTopicConstraint.CountryConstraintList):
            Countries where the resource's domain is not
            covered by the certificates associated with it.

            This field is a member of `oneof`_ ``value``.
    """

    class CountryConstraintList(proto.Message):
        r"""A list of countries where a resource's serving is
        constrained.

        Attributes:
            total_targeted_countries (int):
                Total number of countries targeted by the
                resource.

                This field is a member of `oneof`_ ``_total_targeted_countries``.
            countries (Sequence[google.ads.googleads.v12.common.types.PolicyTopicConstraint.CountryConstraint]):
                Countries in which serving is restricted.
        """

        total_targeted_countries = proto.Field(
            proto.INT32, number=3, optional=True,
        )
        countries = proto.RepeatedField(
            proto.MESSAGE,
            number=2,
            message="PolicyTopicConstraint.CountryConstraint",
        )

    class ResellerConstraint(proto.Message):
        r"""Indicates that a policy topic was constrained due to
        disapproval of the website for reseller purposes.

        """

    class CountryConstraint(proto.Message):
        r"""Indicates that a resource's ability to serve in a particular
        country is constrained.

        Attributes:
            country_criterion (str):
                Geo target constant resource name of the
                country in which serving is constrained.

                This field is a member of `oneof`_ ``_country_criterion``.
        """

        country_criterion = proto.Field(proto.STRING, number=2, optional=True,)

    country_constraint_list = proto.Field(
        proto.MESSAGE, number=1, oneof="value", message=CountryConstraintList,
    )
    reseller_constraint = proto.Field(
        proto.MESSAGE, number=2, oneof="value", message=ResellerConstraint,
    )
    certificate_missing_in_country_list = proto.Field(
        proto.MESSAGE, number=3, oneof="value", message=CountryConstraintList,
    )
    certificate_domain_mismatch_in_country_list = proto.Field(
        proto.MESSAGE, number=4, oneof="value", message=CountryConstraintList,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
