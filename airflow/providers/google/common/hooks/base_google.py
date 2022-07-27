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
import functools
import json
import logging
import os
import tempfile
import warnings
from contextlib import ExitStack, contextmanager
from subprocess import check_output
from typing import Any, Callable, Dict, Generator, Optional, Sequence, Tuple, TypeVar, Union, cast

import google.auth
import google.auth.credentials
import google.oauth2.service_account
import google_auth_httplib2
import requests
import tenacity
from google.api_core.exceptions import Forbidden, ResourceExhausted, TooManyRequests
from google.api_core.gapic_v1.client_info import ClientInfo
from google.auth import _cloud_sdk, compute_engine
from google.auth.environment_vars import CLOUD_SDK_CONFIG_DIR, CREDENTIALS
from google.auth.exceptions import RefreshError
from google.auth.transport import _http_client
from googleapiclient import discovery
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload, build_http, set_user_agent

from airflow import version
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.providers.google.cloud.utils.credentials_provider import (
    _get_scopes,
    _get_target_principal_and_delegates,
    get_credentials_and_project_id,
)
from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.utils.process_utils import patch_environ

log = logging.getLogger(__name__)


# Constants used by the mechanism of repeating requests in reaction to exceeding the temporary quota.
INVALID_KEYS = [
    'DefaultRequestsPerMinutePerProject',
    'DefaultRequestsPerMinutePerUser',
    'RequestsPerMinutePerProject',
    "Resource has been exhausted (e.g. check quota).",
]
INVALID_REASONS = [
    'userRateLimitExceeded',
]


def is_soft_quota_exception(exception: Exception):
    """
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
        return any(reason in error.details() for reason in INVALID_REASONS for error in exception.errors)

    if isinstance(exception, (ResourceExhausted, TooManyRequests)):
        return any(key in error.details() for key in INVALID_KEYS for error in exception.errors)

    return False


def is_operation_in_progress_exception(exception: Exception) -> bool:
    """
    Some of the calls return 429 (too many requests!) or 409 errors (Conflict)
    in case of operation in progress.

    * Google Cloud SQL
    """
    if isinstance(exception, HttpError):
        return exception.resp.status == 429 or exception.resp.status == 409
    return False


class retry_if_temporary_quota(tenacity.retry_if_exception):
    """Retries if there was an exception for exceeding the temporary quote limit."""

    def __init__(self):
        super().__init__(is_soft_quota_exception)


class retry_if_operation_in_progress(tenacity.retry_if_exception):
    """Retries if there was an exception for exceeding the temporary quote limit."""

    def __init__(self):
        super().__init__(is_operation_in_progress_exception)


# A fake project_id to use in functions decorated by fallback_to_default_project_id
# This allows the 'project_id' argument to be of type str instead of Optional[str],
# making it easier to type hint the function body without dealing with the None
# case that can never happen at runtime.
PROVIDE_PROJECT_ID: str = cast(str, None)

T = TypeVar("T", bound=Callable)
RT = TypeVar('RT')


class GoogleBaseHook(BaseHook):
    """
    A base hook for Google cloud-related hooks. Google cloud has a shared REST
    API client that is built in the same way no matter which service you use.
    This class helps construct and authorize the credentials needed to then
    call googleapiclient.discovery.build() to actually discover and build a client
    for a Google cloud service.

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
        domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account.
    """

    conn_name_attr = 'gcp_conn_id'
    default_conn_name = 'google_cloud_default'
    conn_type = 'google_cloud_platform'
    hook_name = 'Google Cloud'

    @staticmethod
    def get_connection_form_widgets() -> Dict[str, Any]:
        """Returns connection widgets to add to connection form"""
        from flask_appbuilder.fieldwidgets import BS3PasswordFieldWidget, BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import IntegerField, PasswordField, StringField
        from wtforms.validators import NumberRange

        return {
            "extra__google_cloud_platform__project": StringField(
                lazy_gettext('Project Id'), widget=BS3TextFieldWidget()
            ),
            "extra__google_cloud_platform__key_path": StringField(
                lazy_gettext('Keyfile Path'), widget=BS3TextFieldWidget()
            ),
            "extra__google_cloud_platform__keyfile_dict": PasswordField(
                lazy_gettext('Keyfile JSON'), widget=BS3PasswordFieldWidget()
            ),
            "extra__google_cloud_platform__scope": StringField(
                lazy_gettext('Scopes (comma separated)'), widget=BS3TextFieldWidget()
            ),
            "extra__google_cloud_platform__key_secret_name": StringField(
                lazy_gettext('Keyfile Secret Name (in GCP Secret Manager)'), widget=BS3TextFieldWidget()
            ),
            "extra__google_cloud_platform__key_secret_project_id": StringField(
                lazy_gettext('Keyfile Secret Project Id (in GCP Secret Manager)'), widget=BS3TextFieldWidget()
            ),
            "extra__google_cloud_platform__num_retries": IntegerField(
                lazy_gettext('Number of Retries'),
                validators=[NumberRange(min=0)],
                widget=BS3TextFieldWidget(),
                default=5,
            ),
        }

    @staticmethod
    def get_ui_field_behaviour() -> Dict[str, Any]:
        """Returns custom field behaviour"""
        return {
            "hidden_fields": ['host', 'schema', 'login', 'password', 'port', 'extra'],
            "relabeling": {},
        }

    def __init__(
        self,
        gcp_conn_id: str = 'google_cloud_default',
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
    ) -> None:
        super().__init__()
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain
        self.extras = self.get_connection(self.gcp_conn_id).extra_dejson  # type: Dict
        self._cached_credentials: Optional[google.auth.credentials.Credentials] = None
        self._cached_project_id: Optional[str] = None

    def _get_credentials_and_project_id(self) -> Tuple[google.auth.credentials.Credentials, Optional[str]]:
        """Returns the Credentials object for Google API and the associated project_id"""
        if self._cached_credentials is not None:
            return self._cached_credentials, self._cached_project_id

        key_path: Optional[str] = self._get_field('key_path', None)
        try:
            keyfile_dict: Optional[str] = self._get_field('keyfile_dict', None)
            keyfile_dict_json: Optional[Dict[str, str]] = None
            if keyfile_dict:
                keyfile_dict_json = json.loads(keyfile_dict)
        except json.decoder.JSONDecodeError:
            raise AirflowException('Invalid key JSON.')
        key_secret_name: Optional[str] = self._get_field('key_secret_name', None)
        key_secret_project_id: Optional[str] = self._get_field('key_secret_project_id', None)

        target_principal, delegates = _get_target_principal_and_delegates(self.impersonation_chain)

        credentials, project_id = get_credentials_and_project_id(
            key_path=key_path,
            keyfile_dict=keyfile_dict_json,
            key_secret_name=key_secret_name,
            key_secret_project_id=key_secret_project_id,
            scopes=self.scopes,
            delegate_to=self.delegate_to,
            target_principal=target_principal,
            delegates=delegates,
        )

        overridden_project_id = self._get_field('project')
        if overridden_project_id:
            project_id = overridden_project_id

        self._cached_credentials = credentials
        self._cached_project_id = project_id

        return credentials, project_id

    def _get_credentials(self) -> google.auth.credentials.Credentials:
        """Returns the Credentials object for Google API"""
        credentials, _ = self._get_credentials_and_project_id()
        return credentials

    def _get_access_token(self) -> str:
        """Returns a valid access token from Google API Credentials"""
        credentials = self._get_credentials()
        auth_req = google.auth.transport.requests.Request()
        # credentials.token is None
        # Need to refresh credentials to populate the token
        credentials.refresh(auth_req)
        return credentials.token

    @functools.lru_cache(maxsize=None)
    def _get_credentials_email(self) -> str:
        """
        Returns the email address associated with the currently logged in account

        If a service account is used, it returns the service account.
        If user authentication (e.g. gcloud auth) is used, it returns the e-mail account of that user.
        """
        credentials = self._get_credentials()

        if isinstance(credentials, compute_engine.Credentials):
            try:
                credentials.refresh(_http_client.Request())
            except RefreshError as msg:
                """
                If the Compute Engine metadata service can't be reached in this case the instance has not
                credentials.
                """
                self.log.debug(msg)

        service_account_email = getattr(credentials, 'service_account_email', None)
        if service_account_email:
            return service_account_email

        http_authorized = self._authorize()
        oauth2_client = discovery.build('oauth2', "v1", http=http_authorized, cache_discovery=False)
        return oauth2_client.tokeninfo().execute()['email']

    def _authorize(self) -> google_auth_httplib2.AuthorizedHttp:
        """
        Returns an authorized HTTP object to be used to build a Google cloud
        service hook connection.
        """
        credentials = self._get_credentials()
        http = build_http()
        http = set_user_agent(http, "airflow/" + version.version)
        authed_http = google_auth_httplib2.AuthorizedHttp(credentials, http=http)
        return authed_http

    def _get_field(self, f: str, default: Any = None) -> Any:
        """
        Fetches a field from extras, and returns it. This is some Airflow
        magic. The google_cloud_platform hook type adds custom UI elements
        to the hook page, which allow admins to specify service_account,
        key_path, etc. They get formatted as shown below.
        """
        long_f = f'extra__google_cloud_platform__{f}'
        if hasattr(self, 'extras') and long_f in self.extras:
            return self.extras[long_f]
        else:
            return default

    @property
    def project_id(self) -> Optional[str]:
        """
        Returns project id.

        :return: id of the project
        :rtype: str
        """
        _, project_id = self._get_credentials_and_project_id()
        return project_id

    @property
    def num_retries(self) -> int:
        """
        Returns num_retries from Connection.

        :return: the number of times each API request should be retried
        :rtype: int
        """
        field_value = self._get_field('num_retries', default=5)
        if field_value is None:
            return 5
        if isinstance(field_value, str) and field_value.strip() == '':
            return 5
        try:
            return int(field_value)
        except ValueError:
            raise AirflowException(
                f"The num_retries field should be a integer. "
                f"Current value: \"{field_value}\" (type: {type(field_value)}). "
                f"Please check the connection configuration."
            )

    @property
    def client_info(self) -> ClientInfo:
        """
        Return client information used to generate a user-agent for API calls.

        It allows for better errors tracking.

        This object is only used by the google-cloud-* libraries that are built specifically for
        the Google Cloud. It is not supported by The Google APIs Python Client that use Discovery
        based APIs.
        """
        warnings.warn(
            "This method is deprecated, please use `airflow.providers.google.common.consts.CLIENT_INFO`.",
            DeprecationWarning,
            stacklevel=2,
        )
        return CLIENT_INFO

    @property
    def scopes(self) -> Sequence[str]:
        """
        Return OAuth 2.0 scopes.

        :return: Returns the scope defined in the connection configuration, or the default scope
        :rtype: Sequence[str]
        """
        scope_value = self._get_field('scope', None)  # type: Optional[str]

        return _get_scopes(scope_value)

    @staticmethod
    def quota_retry(*args, **kwargs) -> Callable:
        """
        A decorator that provides a mechanism to repeat requests in response to exceeding a temporary quote
        limit.
        """

        def decorator(fun: Callable):
            default_kwargs = {
                'wait': tenacity.wait_exponential(multiplier=1, max=100),
                'retry': retry_if_temporary_quota(),
                'before': tenacity.before_log(log, logging.DEBUG),
                'after': tenacity.after_log(log, logging.DEBUG),
            }
            default_kwargs.update(**kwargs)
            return tenacity.retry(*args, **default_kwargs)(fun)

        return decorator

    @staticmethod
    def operation_in_progress_retry(*args, **kwargs) -> Callable[[T], T]:
        """
        A decorator that provides a mechanism to repeat requests in response to
        operation in progress (HTTP 409)
        limit.
        """

        def decorator(fun: T):
            default_kwargs = {
                'wait': tenacity.wait_exponential(multiplier=1, max=300),
                'retry': retry_if_operation_in_progress(),
                'before': tenacity.before_log(log, logging.DEBUG),
                'after': tenacity.after_log(log, logging.DEBUG),
            }
            default_kwargs.update(**kwargs)
            return cast(T, tenacity.retry(*args, **default_kwargs)(fun))

        return decorator

    @staticmethod
    def fallback_to_default_project_id(func: Callable[..., RT]) -> Callable[..., RT]:
        """
        Decorator that provides fallback for Google Cloud project id. If
        the project is None it will be replaced with the project_id from the
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
            if 'project_id' in kwargs:
                kwargs['project_id'] = kwargs['project_id'] or self.project_id
            else:
                kwargs['project_id'] = self.project_id
            if not kwargs['project_id']:
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
        Function decorator that provides a Google Cloud credentials for application supporting Application
        Default Credentials (ADC) strategy.

        It is recommended to use ``provide_gcp_credential_file_as_context`` context manager to limit the
        scope when authorization data is available. Using context manager also
        makes it easier to use multiple connection in one function.
        """

        @functools.wraps(func)
        def wrapper(self: GoogleBaseHook, *args, **kwargs):
            with self.provide_gcp_credential_file_as_context():
                return func(self, *args, **kwargs)

        return cast(T, wrapper)

    @contextmanager
    def provide_gcp_credential_file_as_context(self) -> Generator[Optional[str], None, None]:
        """
        Context manager that provides a Google Cloud credentials for application supporting `Application
        Default Credentials (ADC) strategy <https://cloud.google.com/docs/authentication/production>`__.

        It can be used to provide credentials for external programs (e.g. gcloud) that expect authorization
        file in ``GOOGLE_APPLICATION_CREDENTIALS`` environment variable.
        """
        key_path: Optional[str] = self._get_field('key_path', None)
        keyfile_dict: Optional[str] = self._get_field('keyfile_dict', None)
        if key_path and keyfile_dict:
            raise AirflowException(
                "The `keyfile_dict` and `key_path` fields are mutually exclusive. "
                "Please provide only one value."
            )
        elif key_path:
            if key_path.endswith('.p12'):
                raise AirflowException('Legacy P12 key file are not supported, use a JSON key file.')
            with patch_environ({CREDENTIALS: key_path}):
                yield key_path
        elif keyfile_dict:
            with tempfile.NamedTemporaryFile(mode='w+t') as conf_file:
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
        Provides a separate gcloud configuration with current credentials.

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
            exit_stack.enter_context(patch_environ({CLOUD_SDK_CONFIG_DIR: gcloud_config_tmp}))

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
                    check_output(["gcloud", "config", "set", "auth/client_id", creds_content["client_id"]])
                    # Don't display stdout/stderr for security reason
                    check_output(
                        ["gcloud", "config", "set", "auth/client_secret", creds_content["client_secret"]]
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
    def download_content_from_request(file_handle, request: dict, chunk_size: int) -> None:
        """
        Download media resources.
        Note that  the Python file object is compatible with io.Base and can be used with this class also.

        :param file_handle: io.Base or file object. The stream in which to write the downloaded
            bytes.
        :param request: googleapiclient.http.HttpRequest, the media request to perform in chunks.
        :param chunk_size: int, File will be downloaded in chunks of this many bytes.
        """
        downloader = MediaIoBaseDownload(file_handle, request, chunksize=chunk_size)
        done = False
        while done is False:
            _, done = downloader.next_chunk()
        file_handle.flush()

    def test_connection(self):
        """Test the Google cloud connectivity from UI"""
        status, message = False, ''
        try:
            token = self._get_access_token()
            url = f"https://www.googleapis.com/oauth2/v3/tokeninfo?access_token={token}"
            response = requests.post(url)
            if response.status_code == 200:
                status = True
                message = 'Connection successfully tested'
        except Exception as e:
            status = False
            message = str(e)

        return status, message
