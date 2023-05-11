# Copyright 2020 Google LLC
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
"""A gRPC Interceptor that is responsible to augmenting request metadata.

This class is initialized in the GoogleAdsClient and passed into a grpc
intercept_channel whenever a new service is initialized. It intercepts requests
and updates the metadata in order to insert the developer token and
login-customer-id values.
"""

import pkg_resources

from google.protobuf.internal import api_implementation
from grpc import UnaryUnaryClientInterceptor, UnaryStreamClientInterceptor

from .interceptor import Interceptor

# TODO: This logic should be updated or removed once the following is fixed:
# https://github.com/googleapis/python-api-core/issues/416
try:
    _PROTOBUF_VERSION = pkg_resources.get_distribution("protobuf").version
except pkg_resources.DistributionNotFound:
    # If the distribution can't be found for whatever reason then we set
    # the version to None so that we can know to leave this header out of the
    # request.
    _PROTOBUF_VERSION = None

# Determine which protobuf implementation is being used.
if api_implementation.Type() == "cpp":
    _PB_IMPL_HEADER = "+c"
elif api_implementation.Type() == "python":
    _PB_IMPL_HEADER = "+n"
else:
    _PB_IMPL_HEADER = ""


class MetadataInterceptor(
    Interceptor, UnaryUnaryClientInterceptor, UnaryStreamClientInterceptor
):
    """An interceptor that appends custom metadata to requests."""

    def __init__(
        self, developer_token, login_customer_id, linked_customer_id=None
    ):
        """Initialization method for this class.

        Args:
            developer_token: a str developer token.
            login_customer_id: a str specifying a login customer ID.
            linked_customer_id: a str specifying a linked customer ID.
        """
        self.developer_token_meta = ("developer-token", developer_token)
        self.login_customer_id_meta = (
            ("login-customer-id", login_customer_id)
            if login_customer_id
            else None
        )
        self.linked_customer_id_meta = (
            ("linked-customer-id", linked_customer_id)
            if linked_customer_id
            else None
        )

    def _update_client_call_details_metadata(
        self, client_call_details, metadata
    ):
        """Updates the client call details with additional metadata.

        Args:
            client_call_details: An instance of grpc.ClientCallDetails.
            metadata: Additional metadata defined by GoogleAdsClient.

        Returns:
            An new instance of grpc.ClientCallDetails with additional metadata
            from the GoogleAdsClient.
        """
        client_call_details = self.get_client_call_details_instance(
            client_call_details.method,
            client_call_details.timeout,
            metadata,
            client_call_details.credentials,
        )

        return client_call_details

    def _intercept(self, continuation, client_call_details, request):
        """Generic interceptor used for Unary-Unary and Unary-Stream requests.

        Args:
            continuation: a function to continue the request process.
            client_call_details: a grpc._interceptor._ClientCallDetails
                instance containing request metadata.
            request: a SearchGoogleAdsRequest or SearchGoogleAdsStreamRequest
                message class instance.

        Returns:
            A grpc.Call/grpc.Future instance representing a service response.
        """
        if client_call_details.metadata is None:
            metadata = []
        else:
            metadata = list(client_call_details.metadata)

        metadata.append(self.developer_token_meta)

        if self.login_customer_id_meta:
            metadata.append(self.login_customer_id_meta)

        if self.linked_customer_id_meta:
            metadata.append(self.linked_customer_id_meta)

        # TODO: This logic should be updated or removed once the following is
        # fixed: https://github.com/googleapis/python-api-core/issues/416
        for i, metadatum in enumerate(metadata):
            # Check if the user agent header key is in the current metadatum
            if "x-goog-api-client" in metadatum and _PROTOBUF_VERSION:
                # Convert the tuple to a list so it can be modified.
                metadatum = list(metadatum)
                # Check that "pb" isn't already included in the user agent.
                if "pb" not in metadatum[1]:
                    # Append the protobuf version key value pair to the end of
                    # the string.
                    metadatum[1] += f" pb/{_PROTOBUF_VERSION}{_PB_IMPL_HEADER}"
                    # Convert the metadatum back to a tuple.
                    metadatum = tuple(metadatum)
                    # Splice the metadatum back in its original position in
                    # order to preserve the order of the metadata list.
                    metadata[i] = metadatum
                    # Exit the loop since we already found the user agent.
                    break

        client_call_details = self._update_client_call_details_metadata(
            client_call_details, metadata
        )

        return continuation(client_call_details, request)

    def intercept_unary_unary(self, continuation, client_call_details, request):
        """Intercepts and appends custom metadata for Unary-Unary requests.

        Overrides abstract method defined in grpc.UnaryUnaryClientInterceptor.

        Args:
            continuation: a function to continue the request process.
            client_call_details: a grpc._interceptor._ClientCallDetails
                instance containing request metadata.
            request: a SearchGoogleAdsRequest or SearchGoogleAdsStreamRequest
                message class instance.

        Returns:
            A grpc.Call/grpc.Future instance representing a service response.
        """
        return self._intercept(continuation, client_call_details, request)

    def intercept_unary_stream(
        self, continuation, client_call_details, request
    ):
        """Intercepts and appends custom metadata to Unary-Stream requests.

        Overrides abstract method defined in grpc.UnaryStreamClientInterceptor.

        Args:
            continuation: a function to continue the request process.
            client_call_details: a grpc._interceptor._ClientCallDetails
                instance containing request metadata.
            request: a SearchGoogleAdsRequest or SearchGoogleAdsStreamRequest
                message class instance.

        Returns:
            A grpc.Call/grpc.Future instance representing a service response.
        """
        return self._intercept(continuation, client_call_details, request)
