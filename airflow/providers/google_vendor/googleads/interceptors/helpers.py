# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from copy import deepcopy
from airflow.providers.google_vendor.googleads.util import (
    set_nested_message_field,
    get_nested_attr,
    convert_proto_plus_to_protobuf,
    proto_copy_from,
)

# The keys in this dict represent messages that have fields that may contain
# sensitive information, such as PII like an email address, that shouldn't
# be logged. The values are a list of dot-delimited field paths on the message
# where the sensitive information may exist. Messages listed here will have
# their sensitive fields redacted in logs when transmitted in the following
# scenarios:
#     1. They are returned as part of a Search or SearchStream request.
#     2. They are returned individually in a Get request.
#     3. They are sent to the API as part of a Mutate request.
_MESSAGES_WITH_SENSITIVE_FIELDS = {
    "CustomerUserAccess": ["email_address", "inviter_user_email_address"],
    "CustomerUserAccessInvitation": ["email_address"],
    "MutateCustomerUserAccessRequest": [
        "operation.update.email_address",
        "operation.update.inviter_user_email_address",
    ],
    "ChangeEvent": ["user_email"],
    "CreateCustomerClientRequest": ["email_address"],
    "Feed": ["places_location_feed_data.email_address"],
}

# This is a list of the names of messages that return search results from the
# API. These messages contain other messages that may contain sensitive
# information that needs to be masked before being logged.
_SEARCH_RESPONSE_MESSAGE_NAMES = [
    "SearchGoogleAdsResponse",
    "SearchGoogleAdsStreamResponse",
]


def _copy_message(message):
    """Returns a copy of the given message.

    Args:
        message: An object containing information from an API request
            or response.

    Returns:
        A copy of the given message.
    """
    return deepcopy(message)


def _mask_message_fields(field_list, message, mask):
    """Copies the given message and masks sensitive fields.

    Sensitive fields are given as a list of strings and are overridden
    with the word "REDACTED" to protect PII from being logged.

    Args:
        field_list: A list of strings specifying the fields on the message
            that should be masked.
        message: An object containing information from an API request
            or response.
        mask: A str that should replace the sensitive information in the
            message.

    Returns:
        A new instance of the message object with fields copied and masked
            where necessary.
    """
    copy = _copy_message(message)

    for field_path in field_list:
        try:
            # Only mask the field if it's been set on the message.
            if get_nested_attr(copy, field_path):
                set_nested_message_field(copy, field_path, mask)
        except AttributeError:
            # AttributeError is raised when the field is not defined on the
            # message. In this case there's nothing to mask and the field
            # should be skipped.
            break

    return copy


def _mask_google_ads_search_response(message, mask):
    """Copies and masks sensitive data in a Search response

    Response messages include instances of GoogleAdsSearchResponse and
    GoogleAdsSearchStreamResponse.

    Args:
        message: A SearchGoogleAdsResponse or SearchGoogleAdsStreamResponse
            instance.
        mask: A str that should replace the sensitive information in the
            message.

    Returns:
        A copy of the message with sensitive fields masked.
    """
    copy = _copy_message(message)

    for row in copy.results:
        # Each row is an instance of GoogleAdsRow. The ListFields method
        # returns a list of (FieldDescriptor, value) tuples for all fields in
        # the message which are not empty. If this message is a proto_plus proto
        # then we need to access the native proto to call ListFields. If it's
        # not proto_plus we can assume it's protobuf and can access ListFields
        # directly.
        if hasattr(row, "_pb"):
            row_fields = convert_proto_plus_to_protobuf(row).ListFields()
        else:
            row_fields = row.ListFields()
        for field in row_fields:
            field_descriptor = field[0]
            # field_name is the name of the field on the GoogleAdsRow instance,
            # for example "campaign" or "customer_user_access"
            field_name = field_descriptor.name
            # message_name is the name of the message, similar to the class
            # name, for example "Campaign" or "CustomerUserAccess"
            message_name = field_descriptor.message_type.name
            if message_name in _MESSAGES_WITH_SENSITIVE_FIELDS.keys():
                nested_message = getattr(row, field_name)
                masked_message = _mask_message_fields(
                    _MESSAGES_WITH_SENSITIVE_FIELDS[message_name],
                    nested_message,
                    mask,
                )
                # Overwrites the nested message with an exact copy of itself,
                # where sensitive fields have been masked.
                proto_copy_from(getattr(row, field_name), masked_message)

    return copy


def mask_message(message, mask):
    """Copies and returns a message with sensitive fields masked.

    Args:
        message: An object containing information from an API request
            or response.
        mask: A str that should replace the sensitive information in the
            message.

    Returns:
        A copy of the message instance with sensitive fields masked.
    """
    class_name = message.__class__.__name__

    if class_name in _SEARCH_RESPONSE_MESSAGE_NAMES:
        return _mask_google_ads_search_response(message, mask)
    elif class_name in _MESSAGES_WITH_SENSITIVE_FIELDS.keys():
        sensitive_fields = _MESSAGES_WITH_SENSITIVE_FIELDS[class_name]
        return _mask_message_fields(sensitive_fields, message, mask)
    else:
        return message
