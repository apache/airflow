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
from collections import OrderedDict
import os
import re
from typing import Dict, Optional, Sequence, Tuple, Type, Union
import pkg_resources

from google.api_core import client_options as client_options_lib
from google.api_core import gapic_v1
from google.api_core import retry as retries
from google.auth import credentials as ga_credentials  # type: ignore
from google.auth.transport import mtls  # type: ignore
from google.auth.transport.grpc import SslCredentials  # type: ignore
from google.auth.exceptions import MutualTLSChannelError  # type: ignore
from google.oauth2 import service_account  # type: ignore

try:
    OptionalRetry = Union[retries.Retry, gapic_v1.method._MethodDefault]
except AttributeError:  # pragma: NO COVER
    OptionalRetry = Union[retries.Retry, object]  # type: ignore

from google.ads.googleads.v12.enums.types import audience_insights_dimension
from google.ads.googleads.v12.services.types import audience_insights_service
from .transports.base import (
    AudienceInsightsServiceTransport,
    DEFAULT_CLIENT_INFO,
)
from .transports.grpc import AudienceInsightsServiceGrpcTransport


class AudienceInsightsServiceClientMeta(type):
    """Metaclass for the AudienceInsightsService client.

    This provides class-level methods for building and retrieving
    support objects (e.g. transport) without polluting the client instance
    objects.
    """

    _transport_registry = (
        OrderedDict()
    )  # type: Dict[str, Type[AudienceInsightsServiceTransport]]
    _transport_registry["grpc"] = AudienceInsightsServiceGrpcTransport

    def get_transport_class(
        cls, label: str = None,
    ) -> Type[AudienceInsightsServiceTransport]:
        """Returns an appropriate transport class.

        Args:
            label: The name of the desired transport. If none is
                provided, then the first transport in the registry is used.

        Returns:
            The transport class to use.
        """
        # If a specific transport is requested, return that one.
        if label:
            return cls._transport_registry[label]

        # No transport is requested; return the default (that is, the first one
        # in the dictionary).
        return next(iter(cls._transport_registry.values()))


class AudienceInsightsServiceClient(
    metaclass=AudienceInsightsServiceClientMeta
):
    """Audience Insights Service helps users find information about
    groups of people and how they can be reached with Google Ads.
    """

    @staticmethod
    def _get_default_mtls_endpoint(api_endpoint):
        """Converts api endpoint to mTLS endpoint.

        Convert "*.sandbox.googleapis.com" and "*.googleapis.com" to
        "*.mtls.sandbox.googleapis.com" and "*.mtls.googleapis.com" respectively.
        Args:
            api_endpoint (Optional[str]): the api endpoint to convert.
        Returns:
            str: converted mTLS api endpoint.
        """
        if not api_endpoint:
            return api_endpoint

        mtls_endpoint_re = re.compile(
            r"(?P<name>[^.]+)(?P<mtls>\.mtls)?(?P<sandbox>\.sandbox)?(?P<googledomain>\.googleapis\.com)?"
        )

        m = mtls_endpoint_re.match(api_endpoint)
        name, mtls, sandbox, googledomain = m.groups()
        if mtls or not googledomain:
            return api_endpoint

        if sandbox:
            return api_endpoint.replace(
                "sandbox.googleapis.com", "mtls.sandbox.googleapis.com"
            )

        return api_endpoint.replace(".googleapis.com", ".mtls.googleapis.com")

    DEFAULT_ENDPOINT = "googleads.googleapis.com"
    DEFAULT_MTLS_ENDPOINT = _get_default_mtls_endpoint.__func__(  # type: ignore
        DEFAULT_ENDPOINT
    )

    @classmethod
    def from_service_account_info(cls, info: dict, *args, **kwargs):
        """Creates an instance of this client using the provided credentials
            info.

        Args:
            info (dict): The service account private key info.
            args: Additional arguments to pass to the constructor.
            kwargs: Additional arguments to pass to the constructor.

        Returns:
            AudienceInsightsServiceClient: The constructed client.
        """
        credentials = service_account.Credentials.from_service_account_info(
            info
        )
        kwargs["credentials"] = credentials
        return cls(*args, **kwargs)

    @classmethod
    def from_service_account_file(cls, filename: str, *args, **kwargs):
        """Creates an instance of this client using the provided credentials
        file.

        Args:
            filename (str): The path to the service account private key json
                file.
            args: Additional arguments to pass to the constructor.
            kwargs: Additional arguments to pass to the constructor.

        Returns:
            AudienceInsightsServiceClient: The constructed client.
        """
        credentials = service_account.Credentials.from_service_account_file(
            filename
        )
        kwargs["credentials"] = credentials
        return cls(*args, **kwargs)

    from_service_account_json = from_service_account_file

    @property
    def transport(self) -> AudienceInsightsServiceTransport:
        """Returns the transport used by the client instance.

        Returns:
            AudienceInsightsServiceTransport: The transport used by the client
                instance.
        """
        return self._transport

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        """Releases underlying transport's resources.

        .. warning::
            ONLY use as a context manager if the transport is NOT shared
            with other clients! Exiting the with block will CLOSE the transport
            and may cause errors in other clients!
        """
        self.transport.close()

    @staticmethod
    def common_billing_account_path(billing_account: str,) -> str:
        """Returns a fully-qualified billing_account string."""
        return "billingAccounts/{billing_account}".format(
            billing_account=billing_account,
        )

    @staticmethod
    def parse_common_billing_account_path(path: str) -> Dict[str, str]:
        """Parse a billing_account path into its component segments."""
        m = re.match(r"^billingAccounts/(?P<billing_account>.+?)$", path)
        return m.groupdict() if m else {}

    @staticmethod
    def common_folder_path(folder: str,) -> str:
        """Returns a fully-qualified folder string."""
        return "folders/{folder}".format(folder=folder,)

    @staticmethod
    def parse_common_folder_path(path: str) -> Dict[str, str]:
        """Parse a folder path into its component segments."""
        m = re.match(r"^folders/(?P<folder>.+?)$", path)
        return m.groupdict() if m else {}

    @staticmethod
    def common_organization_path(organization: str,) -> str:
        """Returns a fully-qualified organization string."""
        return "organizations/{organization}".format(organization=organization,)

    @staticmethod
    def parse_common_organization_path(path: str) -> Dict[str, str]:
        """Parse a organization path into its component segments."""
        m = re.match(r"^organizations/(?P<organization>.+?)$", path)
        return m.groupdict() if m else {}

    @staticmethod
    def common_project_path(project: str,) -> str:
        """Returns a fully-qualified project string."""
        return "projects/{project}".format(project=project,)

    @staticmethod
    def parse_common_project_path(path: str) -> Dict[str, str]:
        """Parse a project path into its component segments."""
        m = re.match(r"^projects/(?P<project>.+?)$", path)
        return m.groupdict() if m else {}

    @staticmethod
    def common_location_path(project: str, location: str,) -> str:
        """Returns a fully-qualified location string."""
        return "projects/{project}/locations/{location}".format(
            project=project, location=location,
        )

    @staticmethod
    def parse_common_location_path(path: str) -> Dict[str, str]:
        """Parse a location path into its component segments."""
        m = re.match(
            r"^projects/(?P<project>.+?)/locations/(?P<location>.+?)$", path
        )
        return m.groupdict() if m else {}

    def __init__(
        self,
        *,
        credentials: Optional[ga_credentials.Credentials] = None,
        transport: Union[str, AudienceInsightsServiceTransport, None] = None,
        client_options: Optional[client_options_lib.ClientOptions] = None,
        client_info: gapic_v1.client_info.ClientInfo = DEFAULT_CLIENT_INFO,
    ) -> None:
        """Instantiates the audience insights service client.

        Args:
            credentials (Optional[google.auth.credentials.Credentials]): The
                authorization credentials to attach to requests. These
                credentials identify the application to the service; if none
                are specified, the client will attempt to ascertain the
                credentials from the environment.
            transport (Union[str, AudienceInsightsServiceTransport]): The
                transport to use. If set to None, a transport is chosen
                automatically.
            client_options (google.api_core.client_options.ClientOptions): Custom options for the
                client. It won't take effect if a ``transport`` instance is provided.
                (1) The ``api_endpoint`` property can be used to override the
                default endpoint provided by the client. GOOGLE_API_USE_MTLS_ENDPOINT
                environment variable can also be used to override the endpoint:
                "always" (always use the default mTLS endpoint), "never" (always
                use the default regular endpoint) and "auto" (auto switch to the
                default mTLS endpoint if client certificate is present, this is
                the default value). However, the ``api_endpoint`` property takes
                precedence if provided.
                (2) If GOOGLE_API_USE_CLIENT_CERTIFICATE environment variable
                is "true", then the ``client_cert_source`` property can be used
                to provide client certificate for mutual TLS transport. If
                not provided, the default SSL client certificate will be used if
                present. If GOOGLE_API_USE_CLIENT_CERTIFICATE is "false" or not
                set, no client certificate will be used.
            client_info (google.api_core.gapic_v1.client_info.ClientInfo):
                The client info used to send a user-agent string along with
                API requests. If ``None``, then default info will be used.
                Generally, you only need to set this if you're developing
                your own client library.

        Raises:
            google.auth.exceptions.MutualTLSChannelError: If mutual TLS transport
                creation failed for any reason.
        """
        if isinstance(client_options, dict):
            client_options = client_options_lib.from_dict(client_options)
        if client_options is None:
            client_options = client_options_lib.ClientOptions()

        # Create SSL credentials for mutual TLS if needed.
        if os.getenv("GOOGLE_API_USE_CLIENT_CERTIFICATE", "false") not in (
            "true",
            "false",
        ):
            raise ValueError(
                "Environment variable `GOOGLE_API_USE_CLIENT_CERTIFICATE` must be either `true` or `false`"
            )
        use_client_cert = (
            os.getenv("GOOGLE_API_USE_CLIENT_CERTIFICATE", "false") == "true"
        )

        client_cert_source_func = None
        is_mtls = False
        if use_client_cert:
            if client_options.client_cert_source:
                is_mtls = True
                client_cert_source_func = client_options.client_cert_source
            else:
                is_mtls = mtls.has_default_client_cert_source()
                if is_mtls:
                    client_cert_source_func = mtls.default_client_cert_source()
                else:
                    client_cert_source_func = None

        # Figure out which api endpoint to use.
        if client_options.api_endpoint is not None:
            api_endpoint = client_options.api_endpoint
        else:
            use_mtls_env = os.getenv("GOOGLE_API_USE_MTLS_ENDPOINT", "auto")
            if use_mtls_env == "never":
                api_endpoint = self.DEFAULT_ENDPOINT
            elif use_mtls_env == "always":
                api_endpoint = self.DEFAULT_MTLS_ENDPOINT
            elif use_mtls_env == "auto":
                api_endpoint = (
                    self.DEFAULT_MTLS_ENDPOINT
                    if is_mtls
                    else self.DEFAULT_ENDPOINT
                )
            else:
                raise MutualTLSChannelError(
                    "Unsupported GOOGLE_API_USE_MTLS_ENDPOINT value. Accepted "
                    "values: never, auto, always"
                )

        # Save or instantiate the transport.
        # Ordinarily, we provide the transport, but allowing a custom transport
        # instance provides an extensibility point for unusual situations.
        if isinstance(transport, AudienceInsightsServiceTransport):
            # transport is a AudienceInsightsServiceTransport instance.
            if credentials or client_options.credentials_file:
                raise ValueError(
                    "When providing a transport instance, "
                    "provide its credentials directly."
                )
            if client_options.scopes:
                raise ValueError(
                    "When providing a transport instance, provide its scopes "
                    "directly."
                )
            self._transport = transport
        else:
            Transport = type(self).get_transport_class(transport)
            self._transport = Transport(
                credentials=credentials,
                credentials_file=client_options.credentials_file,
                host=api_endpoint,
                scopes=client_options.scopes,
                client_cert_source_for_mtls=client_cert_source_func,
                quota_project_id=client_options.quota_project_id,
                client_info=client_info,
                always_use_jwt_access=True,
            )

    def generate_insights_finder_report(
        self,
        request: Union[
            audience_insights_service.GenerateInsightsFinderReportRequest, dict
        ] = None,
        *,
        customer_id: str = None,
        baseline_audience: audience_insights_service.BasicInsightsAudience = None,
        specific_audience: audience_insights_service.BasicInsightsAudience = None,
        retry: OptionalRetry = gapic_v1.method.DEFAULT,
        timeout: float = None,
        metadata: Sequence[Tuple[str, str]] = (),
    ) -> audience_insights_service.GenerateInsightsFinderReportResponse:
        r"""Creates a saved report that can be viewed in the Insights Finder
        tool.

        List of thrown errors: `AuthenticationError <>`__
        `AuthorizationError <>`__ `FieldError <>`__ `HeaderError <>`__
        `InternalError <>`__ `QuotaError <>`__ `RangeError <>`__
        `RequestError <>`__

        Args:
            request (Union[google.ads.googleads.v12.services.types.GenerateInsightsFinderReportRequest, dict]):
                The request object. Request message for
                [AudienceInsightsService.GenerateInsightsFinderReport][google.ads.googleads.v12.services.AudienceInsightsService.GenerateInsightsFinderReport].
            customer_id (str):
                Required. The ID of the customer.
                This corresponds to the ``customer_id`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            baseline_audience (google.ads.googleads.v12.services.types.BasicInsightsAudience):
                Required. A baseline audience for
                this report, typically all people in a
                region.

                This corresponds to the ``baseline_audience`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            specific_audience (google.ads.googleads.v12.services.types.BasicInsightsAudience):
                Required. The specific audience of
                interest for this report.  The insights
                in the report will be based on
                attributes more prevalent in this
                audience than in the report's baseline
                audience.

                This corresponds to the ``specific_audience`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            retry (google.api_core.retry.Retry): Designation of what errors, if any,
                should be retried.
            timeout (float): The timeout for this request.
            metadata (Sequence[Tuple[str, str]]): Strings which should be
                sent along with the request as metadata.

        Returns:
            google.ads.googleads.v12.services.types.GenerateInsightsFinderReportResponse:
                The response message for
                   [AudienceInsightsService.GenerateInsightsFinderReport][google.ads.googleads.v12.services.AudienceInsightsService.GenerateInsightsFinderReport],
                   containing the shareable URL for the report.

        """
        # Create or coerce a protobuf request object.
        # Quick check: If we got a request object, we should *not* have
        # gotten any keyword arguments that map to the request.
        has_flattened_params = any(
            [customer_id, baseline_audience, specific_audience]
        )
        if request is not None and has_flattened_params:
            raise ValueError(
                "If the `request` argument is set, then none of "
                "the individual field arguments should be set."
            )

        # Minor optimization to avoid making a copy if the user passes
        # in a audience_insights_service.GenerateInsightsFinderReportRequest.
        # There's no risk of modifying the input as we've already verified
        # there are no flattened fields.
        if not isinstance(
            request,
            audience_insights_service.GenerateInsightsFinderReportRequest,
        ):
            request = audience_insights_service.GenerateInsightsFinderReportRequest(
                request
            )
            # If we have keyword arguments corresponding to fields on the
            # request, apply these.
            if customer_id is not None:
                request.customer_id = customer_id
            if baseline_audience is not None:
                request.baseline_audience = baseline_audience
            if specific_audience is not None:
                request.specific_audience = specific_audience

        # Wrap the RPC method; this adds retry and timeout information,
        # and friendly error handling.
        rpc = self._transport._wrapped_methods[
            self._transport.generate_insights_finder_report
        ]

        # Certain fields should be provided within the metadata header;
        # add these here.
        metadata = tuple(metadata) + (
            gapic_v1.routing_header.to_grpc_metadata(
                (("customer_id", request.customer_id),)
            ),
        )

        # Send the request.
        response = rpc(
            request, retry=retry, timeout=timeout, metadata=metadata,
        )

        # Done; return the response.
        return response

    def list_audience_insights_attributes(
        self,
        request: Union[
            audience_insights_service.ListAudienceInsightsAttributesRequest,
            dict,
        ] = None,
        *,
        customer_id: str = None,
        dimensions: Sequence[
            audience_insights_dimension.AudienceInsightsDimensionEnum.AudienceInsightsDimension
        ] = None,
        query_text: str = None,
        retry: OptionalRetry = gapic_v1.method.DEFAULT,
        timeout: float = None,
        metadata: Sequence[Tuple[str, str]] = (),
    ) -> audience_insights_service.ListAudienceInsightsAttributesResponse:
        r"""Searches for audience attributes that can be used to generate
        insights.

        List of thrown errors: `AuthenticationError <>`__
        `AuthorizationError <>`__ `FieldError <>`__ `HeaderError <>`__
        `InternalError <>`__ `QuotaError <>`__ `RangeError <>`__
        `RequestError <>`__

        Args:
            request (Union[google.ads.googleads.v12.services.types.ListAudienceInsightsAttributesRequest, dict]):
                The request object. Request message for
                [AudienceInsightsService.ListAudienceInsightsAttributes][google.ads.googleads.v12.services.AudienceInsightsService.ListAudienceInsightsAttributes].
            customer_id (str):
                Required. The ID of the customer.
                This corresponds to the ``customer_id`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            dimensions (Sequence[google.ads.googleads.v12.enums.types.AudienceInsightsDimensionEnum.AudienceInsightsDimension]):
                Required. The types of attributes to
                be returned.

                This corresponds to the ``dimensions`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            query_text (str):
                Required. A free text query.
                Attributes matching or related to this
                string will be returned.

                This corresponds to the ``query_text`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            retry (google.api_core.retry.Retry): Designation of what errors, if any,
                should be retried.
            timeout (float): The timeout for this request.
            metadata (Sequence[Tuple[str, str]]): Strings which should be
                sent along with the request as metadata.

        Returns:
            google.ads.googleads.v12.services.types.ListAudienceInsightsAttributesResponse:
                Response message for
                   [AudienceInsightsService.ListAudienceInsightsAttributes][google.ads.googleads.v12.services.AudienceInsightsService.ListAudienceInsightsAttributes].

        """
        # Create or coerce a protobuf request object.
        # Quick check: If we got a request object, we should *not* have
        # gotten any keyword arguments that map to the request.
        has_flattened_params = any([customer_id, dimensions, query_text])
        if request is not None and has_flattened_params:
            raise ValueError(
                "If the `request` argument is set, then none of "
                "the individual field arguments should be set."
            )

        # Minor optimization to avoid making a copy if the user passes
        # in a audience_insights_service.ListAudienceInsightsAttributesRequest.
        # There's no risk of modifying the input as we've already verified
        # there are no flattened fields.
        if not isinstance(
            request,
            audience_insights_service.ListAudienceInsightsAttributesRequest,
        ):
            request = audience_insights_service.ListAudienceInsightsAttributesRequest(
                request
            )
            # If we have keyword arguments corresponding to fields on the
            # request, apply these.
            if customer_id is not None:
                request.customer_id = customer_id
            if dimensions is not None:
                request.dimensions = dimensions
            if query_text is not None:
                request.query_text = query_text

        # Wrap the RPC method; this adds retry and timeout information,
        # and friendly error handling.
        rpc = self._transport._wrapped_methods[
            self._transport.list_audience_insights_attributes
        ]

        # Certain fields should be provided within the metadata header;
        # add these here.
        metadata = tuple(metadata) + (
            gapic_v1.routing_header.to_grpc_metadata(
                (("customer_id", request.customer_id),)
            ),
        )

        # Send the request.
        response = rpc(
            request, retry=retry, timeout=timeout, metadata=metadata,
        )

        # Done; return the response.
        return response

    def list_insights_eligible_dates(
        self,
        request: Union[
            audience_insights_service.ListInsightsEligibleDatesRequest, dict
        ] = None,
        *,
        retry: OptionalRetry = gapic_v1.method.DEFAULT,
        timeout: float = None,
        metadata: Sequence[Tuple[str, str]] = (),
    ) -> audience_insights_service.ListInsightsEligibleDatesResponse:
        r"""Lists date ranges for which audience insights data can be
        requested.

        List of thrown errors: `AuthenticationError <>`__
        `AuthorizationError <>`__ `FieldError <>`__ `HeaderError <>`__
        `InternalError <>`__ `QuotaError <>`__ `RangeError <>`__
        `RequestError <>`__

        Args:
            request (Union[google.ads.googleads.v12.services.types.ListInsightsEligibleDatesRequest, dict]):
                The request object. Request message for
                [AudienceInsightsService.ListAudienceInsightsDates][].
            retry (google.api_core.retry.Retry): Designation of what errors, if any,
                should be retried.
            timeout (float): The timeout for this request.
            metadata (Sequence[Tuple[str, str]]): Strings which should be
                sent along with the request as metadata.

        Returns:
            google.ads.googleads.v12.services.types.ListInsightsEligibleDatesResponse:
                Response message for
                [AudienceInsightsService.ListAudienceInsightsDates][].

        """
        # Create or coerce a protobuf request object.
        # Minor optimization to avoid making a copy if the user passes
        # in a audience_insights_service.ListInsightsEligibleDatesRequest.
        # There's no risk of modifying the input as we've already verified
        # there are no flattened fields.
        if not isinstance(
            request, audience_insights_service.ListInsightsEligibleDatesRequest
        ):
            request = audience_insights_service.ListInsightsEligibleDatesRequest(
                request
            )

        # Wrap the RPC method; this adds retry and timeout information,
        # and friendly error handling.
        rpc = self._transport._wrapped_methods[
            self._transport.list_insights_eligible_dates
        ]

        # Send the request.
        response = rpc(
            request, retry=retry, timeout=timeout, metadata=metadata,
        )

        # Done; return the response.
        return response

    def generate_audience_composition_insights(
        self,
        request: Union[
            audience_insights_service.GenerateAudienceCompositionInsightsRequest,
            dict,
        ] = None,
        *,
        customer_id: str = None,
        audience: audience_insights_service.InsightsAudience = None,
        dimensions: Sequence[
            audience_insights_dimension.AudienceInsightsDimensionEnum.AudienceInsightsDimension
        ] = None,
        retry: OptionalRetry = gapic_v1.method.DEFAULT,
        timeout: float = None,
        metadata: Sequence[Tuple[str, str]] = (),
    ) -> audience_insights_service.GenerateAudienceCompositionInsightsResponse:
        r"""Returns a collection of attributes that are represented in an
        audience of interest, with metrics that compare each attribute's
        share of the audience with its share of a baseline audience.

        List of thrown errors: `AudienceInsightsError <>`__
        `AuthenticationError <>`__ `AuthorizationError <>`__
        `FieldError <>`__ `HeaderError <>`__ `InternalError <>`__
        `QuotaError <>`__ `RangeError <>`__ `RequestError <>`__

        Args:
            request (Union[google.ads.googleads.v12.services.types.GenerateAudienceCompositionInsightsRequest, dict]):
                The request object. Request message for
                [AudienceInsightsService.GenerateAudienceCompositionInsights][google.ads.googleads.v12.services.AudienceInsightsService.GenerateAudienceCompositionInsights].
            customer_id (str):
                Required. The ID of the customer.
                This corresponds to the ``customer_id`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            audience (google.ads.googleads.v12.services.types.InsightsAudience):
                Required. The audience of interest
                for which insights are being requested.

                This corresponds to the ``audience`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            dimensions (Sequence[google.ads.googleads.v12.enums.types.AudienceInsightsDimensionEnum.AudienceInsightsDimension]):
                Required. The audience dimensions for
                which composition insights should be
                returned.

                This corresponds to the ``dimensions`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            retry (google.api_core.retry.Retry): Designation of what errors, if any,
                should be retried.
            timeout (float): The timeout for this request.
            metadata (Sequence[Tuple[str, str]]): Strings which should be
                sent along with the request as metadata.

        Returns:
            google.ads.googleads.v12.services.types.GenerateAudienceCompositionInsightsResponse:
                Response message for
                   [AudienceInsightsService.GenerateAudienceCompositionInsights][google.ads.googleads.v12.services.AudienceInsightsService.GenerateAudienceCompositionInsights].

        """
        # Create or coerce a protobuf request object.
        # Quick check: If we got a request object, we should *not* have
        # gotten any keyword arguments that map to the request.
        has_flattened_params = any([customer_id, audience, dimensions])
        if request is not None and has_flattened_params:
            raise ValueError(
                "If the `request` argument is set, then none of "
                "the individual field arguments should be set."
            )

        # Minor optimization to avoid making a copy if the user passes
        # in a audience_insights_service.GenerateAudienceCompositionInsightsRequest.
        # There's no risk of modifying the input as we've already verified
        # there are no flattened fields.
        if not isinstance(
            request,
            audience_insights_service.GenerateAudienceCompositionInsightsRequest,
        ):
            request = audience_insights_service.GenerateAudienceCompositionInsightsRequest(
                request
            )
            # If we have keyword arguments corresponding to fields on the
            # request, apply these.
            if customer_id is not None:
                request.customer_id = customer_id
            if audience is not None:
                request.audience = audience
            if dimensions is not None:
                request.dimensions = dimensions

        # Wrap the RPC method; this adds retry and timeout information,
        # and friendly error handling.
        rpc = self._transport._wrapped_methods[
            self._transport.generate_audience_composition_insights
        ]

        # Certain fields should be provided within the metadata header;
        # add these here.
        metadata = tuple(metadata) + (
            gapic_v1.routing_header.to_grpc_metadata(
                (("customer_id", request.customer_id),)
            ),
        )

        # Send the request.
        response = rpc(
            request, retry=retry, timeout=timeout, metadata=metadata,
        )

        # Done; return the response.
        return response


try:
    DEFAULT_CLIENT_INFO = gapic_v1.client_info.ClientInfo(
        gapic_version=pkg_resources.get_distribution("google-ads",).version,
    )
except pkg_resources.DistributionNotFound:
    DEFAULT_CLIENT_INFO = gapic_v1.client_info.ClientInfo()


__all__ = ("AudienceInsightsServiceClient",)
