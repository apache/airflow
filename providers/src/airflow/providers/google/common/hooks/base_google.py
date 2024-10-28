#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""This module contains a Google Cloud API base hook."""

from __future__ import annotations

import asyncio
import datetime
import functools
import json
import logging
import os
import tempfile
from contextlib import ExitStack, contextmanager
from subprocess import check_output
from typing import TYPE_CHECKING, Any, Callable, Generator, Sequence, TypeVar, cast

import google.auth
import google.oauth2.service_account
import google_auth_httplib2
import requests
import tenacity
from asgiref.sync import sync_to_async
from gcloud.aio.auth.token import Token, TokenResponse
from google.api_core.exceptions import Forbidden, ResourceExhausted, TooManyRequests
from google.auth import _cloud_sdk, compute_engine  # type: ignore[attr-defined]
from google.auth.environment_vars import CLOUD_SDK_CONFIG_DIR, CREDENTIALS
from google.auth.exceptions import RefreshError
from google.auth.transport import _http_client
from googleapiclient import discovery
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload, build_http, set_user_agent
from requests import Session

from airflow import version
from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
from airflow.hooks.base import BaseHook
from airflow.providers.google.cloud.utils.credentials_provider import (
    _get_scopes,
    _get_target_principal_and_delegates,
    get_credentials_and_project_id,
)
from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.providers.google.common.deprecated import deprecated
from airflow.utils.process_utils import patch_environ

if TYPE_CHECKING:
    from aiohttp import ClientSession
    from google.api_core.gapic_v1.client_info import ClientInfo
    from google.auth.credentials import Credentials

log = logging.getLogger(__name__)

# Constants used by the mechanism of repeating requests in reaction to exceeding the temporary quota.
INVALID_KEYS = [
    "DefaultRequestsPerMinutePerProject",
    "DefaultRequestsPerMinutePerUser",
    "RequestsPerMinutePerProject",
    "Resource has been exhausted (e.g. check quota).",
]
INVALID_REASONS = [
    "userRateLimitExceeded",
]


def is_soft_quota_exception(exception: Exception):
    """
    Check for quota violation errors.

    API for Google services does not have a standardized way to report quota violation errors.

    The function has been adapted by trial and error to the following services:
    * Google Translate
    * Google Vision
    * Google Text-to-Speech
    * Google Speech-to-Text
    * Google Natural Language
    * Google Video Intelligence
    """
    if isinstance(exception, Forbidden):
        return any(
            reason in error.details()
            for reason in INVALID_REASONS
            for error in exception.errors
        )

    if isinstance(exception, (ResourceExhausted, TooManyRequests)):
        return any(
            key in error.details() for key in INVALID_KEYS for error in exception.errors
        )

    return False


def is_operation_in_progress_exception(exception: Exception) -> bool:
    """
    Handle operation in-progress exceptions.

    Some calls return 429 (too many requests!) or 409 errors (Conflict) in case of operation in progress.

    * Google Cloud SQL
    """
    if isinstance(exception, HttpError):
        return exception.resp.status == 429 or exception.resp.status == 409
    return False


def is_refresh_credentials_exception(exception: Exception) -> bool:
    """
    Handle refresh credentials exceptions.

    Some calls return 502 (server error) in case a new token cannot be obtained.

    * Google BigQuery
    """
    if isinstance(exception, RefreshError):
        return "Unable to acquire impersonated credentials" in str(exception)
    return False


class retry_if_temporary_quota(tenacity.retry_if_exception):
    """Retries if there was an exception for exceeding the temporary quote limit."""

    def __init__(self):
        super().__init__(is_soft_quota_exception)


class retry_if_operation_in_progress(tenacity.retry_if_exception):
    """Retries if there was an exception in case of operation in progress."""

    def __init__(self):
        super().__init__(is_operation_in_progress_exception)


class retry_if_temporary_refresh_credentials(tenacity.retry_if_exception):
    """Retries if there was an exception for refreshing credentials."""

    def __init__(self):
        super().__init__(is_refresh_credentials_exception)


# A fake project_id to use in functions decorated by fallback_to_default_project_id
# This allows the 'project_id' argument to be of type str instead of str | None,
# making it easier to type hint the function body without dealing with the None
# case that can never happen at runtime.
PROVIDE_PROJECT_ID: str = cast(str, None)

T = TypeVar("T", bound=Callable)
RT = TypeVar("RT")


def get_field(extras: dict, field_name: str):
    """Get field from extra, first checking short name, then for backcompat we check for prefixed name."""
    if field_name.startswith("extra__"):
        raise ValueError(
            f"Got prefixed name {field_name}; please remove the 'extra__google_cloud_platform__' prefix "
            "when using this method."
        )
    if field_name in extras:
        return extras[field_name] or None
    prefixed_name = f"extra__google_cloud_platform__{field_name}"
    return extras.get(prefixed_name) or None


class GoogleBaseHook(BaseHook):
    """
    A base hook for Google cloud-related hooks.

    Google cloud has a shared REST API client that is built in the same way no matter
    which service you use.  This class helps construct and authorize the credentials
    needed to then call googleapiclient.discovery.build() to actually discover and
    build a client for a Google cloud service.

    The class also contains some miscellaneous helper functions.

    All hook derived from this base hook use the 'Google Cloud' connection
    type. Three ways of authentication are supported:

    Default credentials: Only the 'Project Id' is required. You'll need to
    have set up default credentials, such as by the
    ``GOOGLE_APPLICATION_DEFAULT`` environment variable or from the metadata
    server on Google Compute Engine.

    JSON key file: Specify 'Project Id', 'Keyfile Path' and 'Scope'.

    Legacy P12 key files are not supported.

    JSON data provided in the UI: Specify 'Keyfile JSON'.

    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled. The usage of this parameter should be limited only to Google Workspace
        (gsuite) and marketing platform operators and hooks. It is deprecated for usage by Google Cloud
        and Firebase operators and hooks, as well as transfer operators in other providers that involve
        Google cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account.
    """

    conn_name_attr = "gcp_conn_id"
    default_conn_name = "google_cloud_default"
    conn_type = "google_cloud_platform"
    hook_name = "Google Cloud"

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to connection form."""
        from flask_appbuilder.fieldwidgets import (
            BS3PasswordFieldWidget,
            BS3TextFieldWidget,
        )
        from flask_babel import lazy_gettext
        from wtforms import BooleanField, IntegerField, PasswordField, StringField
        from wtforms.validators import NumberRange

        return {
            "project": StringField(
                lazy_gettext("Project Id"), widget=BS3TextFieldWidget()
            ),
            "key_path": StringField(
                lazy_gettext("Keyfile Path"), widget=BS3TextFieldWidget()
            ),
            "keyfile_dict": PasswordField(
                lazy_gettext("Keyfile JSON"), widget=BS3PasswordFieldWidget()
            ),
            "credential_config_file": StringField(
                lazy_gettext("Credential Configuration File"), widget=BS3TextFieldWidget()
            ),
            "scope": StringField(
                lazy_gettext("Scopes (comma separated)"), widget=BS3TextFieldWidget()
            ),
            "key_secret_name": StringField(
                lazy_gettext("Keyfile Secret Name (in GCP Secret Manager)"),
                widget=BS3TextFieldWidget(),
            ),
            "key_secret_project_id": StringField(
                lazy_gettext("Keyfile Secret Project Id (in GCP Secret Manager)"),
                widget=BS3TextFieldWidget(),
            ),
            "num_retries": IntegerField(
                lazy_gettext("Number of Retries"),
                validators=[NumberRange(min=0)],
                widget=BS3TextFieldWidget(),
                default=5,
            ),
            "impersonation_chain": StringField(
                lazy_gettext("Impersonation Chain"), widget=BS3TextFieldWidget()
            ),
            "idp_issuer_url": StringField(
                lazy_gettext("IdP Token Issue URL (Client Credentials Grant Flow)"),
                widget=BS3TextFieldWidget(),
            ),
            "client_id": StringField(
                lazy_gettext("Client ID (Client Credentials Grant Flow)"),
                widget=BS3TextFieldWidget(),
            ),
            "client_secret": StringField(
                lazy_gettext("Client Secret (Client Credentials Grant Flow)"),
                widget=BS3PasswordFieldWidget(),
            ),
            "idp_extra_parameters": StringField(
                lazy_gettext("IdP Extra Request Parameters"), widget=BS3TextFieldWidget()
            ),
            "is_anonymous": BooleanField(
                lazy_gettext("Anonymous credentials (ignores all other settings)"),
                default=False,
            ),
        }

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour."""
        return {
            "hidden_fields": ["host", "schema", "login", "password", "port", "extra"],
            "relabeling": {},
        }

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
    ) -> None:
        super().__init__()
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain
        self.extras: dict = self.get_connection(self.gcp_conn_id).extra_dejson
        self._cached_credentials: Credentials | None = None
        self._cached_project_id: str | None = None

    def get_credentials_and_project_id(self) -> tuple[Credentials, str | None]:
        """Return the Credentials object for Google API and the associated project_id."""
        if self._cached_credentials is not None:
            return self._cached_credentials, self._cached_project_id

        key_path: str | None = self._get_field("key_path", None)
        try:
            keyfile_dict: str | dict[str, str] | None = self._get_field(
                "keyfile_dict", None
            )
            keyfile_dict_json: dict[str, str] | None = None
            if keyfile_dict:
                if isinstance(keyfile_dict, dict):
                    keyfile_dict_json = keyfile_dict
                else:
                    keyfile_dict_json = json.loads(keyfile_dict)
        except json.decoder.JSONDecodeError:
            raise AirflowException("Invalid key JSON.")

        key_secret_name: str | None = self._get_field("key_secret_name", None)
        key_secret_project_id: str | None = self._get_field("key_secret_project_id", None)

        credential_config_file: str | None = self._get_field(
            "credential_config_file", None
        )

        if not self.impersonation_chain:
            self.impersonation_chain = self._get_field("impersonation_chain", None)
            if (
                isinstance(self.impersonation_chain, str)
                and "," in self.impersonation_chain
            ):
                self.impersonation_chain = [
                    s.strip() for s in self.impersonation_chain.split(",")
                ]

        target_principal, delegates = _get_target_principal_and_delegates(
            self.impersonation_chain
        )
        is_anonymous = self._get_field("is_anonymous")

        idp_issuer_url: str | None = self._get_field("idp_issuer_url", None)
        client_id: str | None = self._get_field("client_id", None)
        client_secret: str | None = self._get_field("client_secret", None)
        idp_extra_params: str | None = self._get_field("idp_extra_params", None)

        idp_extra_params_dict: dict[str, str] | None = None
        if idp_extra_params:
            try:
                idp_extra_params_dict = json.loads(idp_extra_params)
            except json.decoder.JSONDecodeError:
                raise AirflowException("Invalid JSON.")

        credentials, project_id = get_credentials_and_project_id(
            key_path=key_path,
            keyfile_dict=keyfile_dict_json,
            credential_config_file=credential_config_file,
            key_secret_name=key_secret_name,
            key_secret_project_id=key_secret_project_id,
            scopes=self.scopes,
            delegate_to=self.delegate_to,
            target_principal=target_principal,
            delegates=delegates,
            is_anonymous=is_anonymous,
            idp_issuer_url=idp_issuer_url,
            client_id=client_id,
            client_secret=client_secret,
            idp_extra_params_dict=idp_extra_params_dict,
        )

        overridden_project_id = self._get_field("project")
        if overridden_project_id:
            project_id = overridden_project_id

        self._cached_credentials = credentials
        self._cached_project_id = project_id

        return credentials, project_id

    def get_credentials(self) -> Credentials:
        """Return the Credentials object for Google API."""
        credentials, _ = self.get_credentials_and_project_id()
        return credentials

    def _get_access_token(self) -> str:
        """Return a valid access token from Google API Credentials."""
        credentials = self.get_credentials()
        auth_req = google.auth.transport.requests.Request()
        # credentials.token is None
        # Need to refresh credentials to populate the token
        credentials.refresh(auth_req)
        return credentials.token

    @functools.cached_property
    def _get_credentials_email(self) -> str:
        """
        Return the email address associated with the currently logged in account.

        If a service account is used, it returns the service account.
        If user authentication (e.g. gcloud auth) is used, it returns the e-mail account of that user.
        """
        credentials = self.get_credentials()

        if isinstance(credentials, compute_engine.Credentials):
            try:
                credentials.refresh(_http_client.Request())
            except RefreshError as msg:
                """
                If the Compute Engine metadata service can't be reached in this case the instance has not
                credentials.
                """
                self.log.debug(msg)

        service_account_email = getattr(credentials, "service_account_email", None)
        if service_account_email:
            return service_account_email

        http_authorized = self._authorize()
        oauth2_client = discovery.build(
            "oauth2", "v1", http=http_authorized, cache_discovery=False
        )
        return oauth2_client.tokeninfo().execute()["email"]

    def _authorize(self) -> google_auth_httplib2.AuthorizedHttp:
        """Return an authorized HTTP object to be used to build a Google cloud service hook connection."""
        credentials = self.get_credentials()
        http = build_http()
        http = set_user_agent(http, "airflow/" + version.version)
        authed_http = google_auth_httplib2.AuthorizedHttp(credentials, http=http)
        return authed_http

    def _get_field(self, f: str, default: Any = None) -> Any:
        """
        Fetch a field from extras, and returns it.

        This is some Airflow magic. The google_cloud_platform hook type adds
        custom UI elements to the hook page, which allow admins to specify
        service_account, key_path, etc. They get formatted as shown below.
        """
        return hasattr(self, "extras") and get_field(self.extras, f) or default

    @property
    def project_id(self) -> str:
        """
        Returns project id.

        :return: id of the project
        """
        _, project_id = self.get_credentials_and_project_id()
        return project_id or PROVIDE_PROJECT_ID

    @property
    def num_retries(self) -> int:
        """
        Returns num_retries from Connection.

        :return: the number of times each API request should be retried
        """
        field_value = self._get_field("num_retries", default=5)
        if field_value is None:
            return 5
        if isinstance(field_value, str) and field_value.strip() == "":
            return 5
        try:
            return int(field_value)
        except ValueError:
            raise AirflowException(
                f"The num_retries field should be a integer. "
                f'Current value: "{field_value}" (type: {type(field_value)}). '
                f"Please check the connection configuration."
            )

    @property
    @deprecated(
        planned_removal_date="March 01, 2025",
        use_instead="airflow.providers.google.common.consts.CLIENT_INFO",
        category=AirflowProviderDeprecationWarning,
    )
    def client_info(self) -> ClientInfo:
        """
        Return client information used to generate a user-agent for API calls.

        It allows for better errors tracking.

        This object is only used by the google-cloud-* libraries that are built specifically for
        the Google Cloud. It is not supported by The Google APIs Python Client that use Discovery
        based APIs.
        """
        return CLIENT_INFO

    @property
    def scopes(self) -> Sequence[str]:
        """
        Return OAuth 2.0 scopes.

        :return: Returns the scope defined in the connection configuration, or the default scope
        """
        scope_value: str | None = self._get_field("scope", None)

        return _get_scopes(scope_value)

    @staticmethod
    def quota_retry(*args, **kwargs) -> Callable:
        """Provide a mechanism to repeat requests in response to exceeding a temporary quota limit."""

        def decorator(func: Callable):
            default_kwargs = {
                "wait": tenacity.wait_exponential(multiplier=1, max=100),
                "retry": retry_if_temporary_quota(),
                "before": tenacity.before_log(log, logging.DEBUG),
                "after": tenacity.after_log(log, logging.DEBUG),
            }
            default_kwargs.update(**kwargs)
            return tenacity.retry(*args, **default_kwargs)(func)

        return decorator

    @staticmethod
    def operation_in_progress_retry(*args, **kwargs) -> Callable[[T], T]:
        """Provide a mechanism to repeat requests in response to operation in progress (HTTP 409) limit."""

        def decorator(func: T):
            default_kwargs = {
                "wait": tenacity.wait_exponential(multiplier=1, max=300),
                "retry": retry_if_operation_in_progress(),
                "before": tenacity.before_log(log, logging.DEBUG),
                "after": tenacity.after_log(log, logging.DEBUG),
            }
            default_kwargs.update(**kwargs)
            return cast(T, tenacity.retry(*args, **default_kwargs)(func))

        return decorator

    @staticmethod
    def refresh_credentials_retry(*args, **kwargs) -> Callable[[T], T]:
        """Provide a mechanism to repeat requests in response to a temporary refresh credential issue."""

        def decorator(func: T):
            default_kwargs = {
                "wait": tenacity.wait_exponential(multiplier=1, max=5),
                "stop": tenacity.stop_after_attempt(3),
                "retry": retry_if_temporary_refresh_credentials(),
                "reraise": True,
                "before": tenacity.before_log(log, logging.DEBUG),
                "after": tenacity.after_log(log, logging.DEBUG),
            }
            default_kwargs.update(**kwargs)
            return cast(T, tenacity.retry(*args, **default_kwargs)(func))

        return decorator

    @staticmethod
    def fallback_to_default_project_id(func: Callable[..., RT]) -> Callable[..., RT]:
        """
        Provide fallback for Google Cloud project id. To be used as a decorator.

        If the project is None it will be replaced with the project_id from the
        service account the Hook is authenticated with. Project id can be specified
        either via project_id kwarg or via first parameter in positional args.

        :param func: function to wrap
        :return: result of the function call
        """

        @functools.wraps(func)
        def inner_wrapper(self: GoogleBaseHook, *args, **kwargs) -> RT:
            if args:
                raise AirflowException(
                    "You must use keyword arguments in this methods rather than positional"
                )
            if "project_id" in kwargs:
                kwargs["project_id"] = kwargs["project_id"] or self.project_id
            else:
                kwargs["project_id"] = self.project_id
            if not kwargs["project_id"]:
                raise AirflowException(
                    "The project id must be passed either as "
                    "keyword project_id parameter or as project_id extra "
                    "in Google Cloud connection definition. Both are not set!"
                )
            return func(self, *args, **kwargs)

        return inner_wrapper

    @staticmethod
    def provide_gcp_credential_file(func: T) -> T:
        """
        Provide a Google Cloud credentials for Application Default Credentials (ADC) strategy support.

        It is recommended to use ``provide_gcp_credential_file_as_context`` context
        manager to limit the scope when authorization data is available. Using context
        manager also makes it easier to use multiple connection in one function.
        """

        @functools.wraps(func)
        def wrapper(self: GoogleBaseHook, *args, **kwargs):
            with self.provide_gcp_credential_file_as_context():
                return func(self, *args, **kwargs)

        return cast(T, wrapper)

    @contextmanager
    def provide_gcp_credential_file_as_context(self) -> Generator[str | None, None, None]:
        """
        Provide a Google Cloud credentials for Application Default Credentials (ADC) strategy support.

        See:
            `Application Default Credentials (ADC)
            strategy <https://cloud.google.com/docs/authentication/production>`__.

        It can be used to provide credentials for external programs (e.g. gcloud) that expect authorization
        file in ``GOOGLE_APPLICATION_CREDENTIALS`` environment variable.
        """
        key_path: str | None = self._get_field("key_path", None)
        keyfile_dict: str | dict[str, str] | None = self._get_field("keyfile_dict", None)
        if key_path and keyfile_dict:
            raise AirflowException(
                "The `keyfile_dict` and `key_path` fields are mutually exclusive. "
                "Please provide only one value."
            )
        elif key_path:
            if key_path.endswith(".p12"):
                raise AirflowException(
                    "Legacy P12 key file are not supported, use a JSON key file."
                )
            with patch_environ({CREDENTIALS: key_path}):
                yield key_path
        elif keyfile_dict:
            with tempfile.NamedTemporaryFile(mode="w+t") as conf_file:
                if isinstance(keyfile_dict, dict):
                    keyfile_dict = json.dumps(keyfile_dict)
                conf_file.write(keyfile_dict)
                conf_file.flush()
                with patch_environ({CREDENTIALS: conf_file.name}):
                    yield conf_file.name
        else:
            # We will use the default service account credentials.
            yield None

    @contextmanager
    def provide_authorized_gcloud(self) -> Generator[None, None, None]:
        """
        Provide a separate gcloud configuration with current credentials.

        The gcloud tool allows you to login to Google Cloud only - ``gcloud auth login`` and
        for the needs of Application Default Credentials ``gcloud auth application-default login``.
        In our case, we want all commands to use only the credentials from ADCm so
        we need to configure the credentials in gcloud manually.
        """
        credentials_path = _cloud_sdk.get_application_default_credentials_path()
        project_id = self.project_id

        with ExitStack() as exit_stack:
            exit_stack.enter_context(self.provide_gcp_credential_file_as_context())
            gcloud_config_tmp = exit_stack.enter_context(tempfile.TemporaryDirectory())
            exit_stack.enter_context(
                patch_environ({CLOUD_SDK_CONFIG_DIR: gcloud_config_tmp})
            )

            if CREDENTIALS in os.environ:
                # This solves most cases when we are logged in using the service key in Airflow.
                # Don't display stdout/stderr for security reason
                check_output(
                    [
                        "gcloud",
                        "auth",
                        "activate-service-account",
                        f"--key-file={os.environ[CREDENTIALS]}",
                    ]
                )
            elif os.path.exists(credentials_path):
                # If we are logged in by `gcloud auth application-default` then we need to log in manually.
                # This will make the `gcloud auth application-default` and `gcloud auth` credentials equals.
                with open(credentials_path) as creds_file:
                    creds_content = json.loads(creds_file.read())
                    # Don't display stdout/stderr for security reason
                    check_output(
                        [
                            "gcloud",
                            "config",
                            "set",
                            "auth/client_id",
                            creds_content["client_id"],
                        ]
                    )
                    # Don't display stdout/stderr for security reason
                    check_output(
                        [
                            "gcloud",
                            "config",
                            "set",
                            "auth/client_secret",
                            creds_content["client_secret"],
                        ]
                    )
                    # Don't display stdout/stderr for security reason
                    check_output(
                        [
                            "gcloud",
                            "auth",
                            "activate-refresh-token",
                            creds_content["client_id"],
                            creds_content["refresh_token"],
                        ]
                    )

            if project_id:
                # Don't display stdout/stderr for security reason
                check_output(["gcloud", "config", "set", "core/project", project_id])

            yield

    @staticmethod
    def download_content_from_request(
        file_handle, request: dict, chunk_size: int
    ) -> None:
        """
        Download media resources.

        Note that the Python file object is compatible with io.Base and can be used with this class also.

        :param file_handle: io.Base or file object. The stream in which to write the downloaded bytes.
        :param request: googleapiclient.http.HttpRequest, the media request to perform in chunks.
        :param chunk_size: int, File will be downloaded in chunks of this many bytes.
        """
        downloader = MediaIoBaseDownload(file_handle, request, chunksize=chunk_size)
        done = False
        while done is False:
            _, done = downloader.next_chunk()
        file_handle.flush()

    def test_connection(self):
        """Test the Google cloud connectivity from UI."""
        status, message = False, ""
        if self._get_field("is_anonymous"):
            return True, "Credentials are anonymous"
        try:
            token = self._get_access_token()
            url = f"https://www.googleapis.com/oauth2/v3/tokeninfo?access_token={token}"
            response = requests.post(url)
            if response.status_code == 200:
                status = True
                message = "Connection successfully tested"
        except Exception as e:
            status = False
            message = str(e)

        return status, message


class _CredentialsToken(Token):
    """
    A token implementation which makes Google credentials objects accessible to [gcloud-aio](https://talkiq.github.io/gcloud-aio/) clients.

    This class allows us to create token instances from credentials objects and thus supports a variety of use cases for Google
    credentials in Airflow (i.e. impersonation chain). By relying on a existing credentials object we leverage functionality provided by the GoogleBaseHook
    for generating credentials objects.
    """

    def __init__(
        self,
        credentials: Credentials,
        *,
        project: str | None = None,
        session: ClientSession | None = None,
        scopes: Sequence[str] | None = None,
    ) -> None:
        _scopes: list[str] | None = list(scopes) if scopes else None
        super().__init__(session=cast(Session, session), scopes=_scopes)
        self.credentials = credentials
        self.project = project

    @classmethod
    async def from_hook(
        cls,
        hook: GoogleBaseHook,
        *,
        session: ClientSession | None = None,
    ) -> _CredentialsToken:
        credentials, project = hook.get_credentials_and_project_id()
        return cls(
            credentials=credentials,
            project=project,
            session=session,
            scopes=hook.scopes,
        )

    async def get_project(self) -> str | None:
        return self.project

    async def refresh(self, *, timeout: int) -> TokenResponse:
        await sync_to_async(self.credentials.refresh)(
            google.auth.transport.requests.Request()
        )

        self.access_token = cast(str, self.credentials.token)
        self.access_token_duration = 3600
        self.access_token_acquired_at = self._now()
        return TokenResponse(
            value=self.access_token, expires_in=self.access_token_duration
        )

    async def acquire_access_token(self, timeout: int = 10) -> None:
        await self.refresh(timeout=timeout)
        self.acquiring = None

    async def ensure_token(self) -> None:
        if self.acquiring and not self.acquiring.done():
            await self.acquiring
            return

        if self.access_token:
            delta = (self._now() - self.access_token_acquired_at).total_seconds()
            if delta <= self.access_token_duration / 2:
                return

        self.acquiring = asyncio.ensure_future(  # pylint: disable=used-before-assignment
            self.acquire_access_token()
        )
        await self.acquiring

    @staticmethod
    def _now():
        # access_token_acquired_at is specific to gcloud-aio's Token.
        # On subsequent calls of `get` it will be used with `datetime.datetime.utcnow()`.
        # Therefore we have to use an offset-naive datetime.
        # https://github.com/talkiq/gcloud-aio/blob/f1132b005ba35d8059229a9ca88b90f31f77456d/auth/gcloud/aio/auth/token.py#L204
        return datetime.datetime.now(tz=datetime.timezone.utc).replace(tzinfo=None)


class GoogleBaseAsyncHook(BaseHook):
    """GoogleBaseAsyncHook inherits from BaseHook class, run on the trigger worker."""

    sync_hook_class: Any = None

    def __init__(self, **kwargs: Any) -> None:
        # add default value to gcp_conn_id
        if "gcp_conn_id" not in kwargs:
            kwargs["gcp_conn_id"] = "google_cloud_default"

        self._hook_kwargs = kwargs
        self._sync_hook = None

    async def get_sync_hook(self) -> Any:
        """Sync version of the Google Cloud Hook makes blocking calls in ``__init__``; don't inherit it."""
        if not self._sync_hook:
            self._sync_hook = await sync_to_async(self.sync_hook_class)(
                **self._hook_kwargs
            )
        return self._sync_hook

    async def get_token(
        self, *, session: ClientSession | None = None
    ) -> _CredentialsToken:
        """Return a Token instance for use in [gcloud-aio](https://talkiq.github.io/gcloud-aio/) clients."""
        sync_hook = await self.get_sync_hook()
        return await _CredentialsToken.from_hook(sync_hook, session=session)

    async def service_file_as_context(self) -> Any:
        """
        Provide a Google Cloud credentials for Application Default Credentials (ADC) strategy support.

        This is the async equivalent of the non-async GoogleBaseHook's `provide_gcp_credential_file_as_context` method.
        """
        sync_hook = await self.get_sync_hook()
        return await sync_to_async(sync_hook.provide_gcp_credential_file_as_context)()
