# -*- coding: utf-8 -*-
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
#
"""
This module contains a Google Cloud API base hook.
"""

import json
import functools
import os
import tempfile
from typing import Any, Optional, Dict, Callable, TypeVar, Sequence

import httplib2

import google.auth
import google.oauth2.service_account
from google.api_core.gapic_v1.client_info import ClientInfo
from google.api_core.exceptions import GoogleAPICallError, AlreadyExists, RetryError
from google.auth.environment_vars import CREDENTIALS

import google_auth_httplib2
from googleapiclient.errors import HttpError

from airflow import version
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook


_DEFAULT_SCOPES = ('https://www.googleapis.com/auth/cloud-platform',)  # type: Sequence[str]


RT = TypeVar('RT')  # pylint: disable=invalid-name


class GoogleCloudBaseHook(BaseHook):
    """
    A base hook for Google cloud-related hooks. Google cloud has a shared REST
    API client that is built in the same way no matter which service you use.
    This class helps construct and authorize the credentials needed to then
    call googleapiclient.discovery.build() to actually discover and build a client
    for a Google cloud service.

    The class also contains some miscellaneous helper functions.

    All hook derived from this base hook use the 'Google Cloud Platform' connection
    type. Three ways of authentication are supported:

    Default credentials: Only the 'Project Id' is required. You'll need to
    have set up default credentials, such as by the
    ``GOOGLE_APPLICATION_DEFAULT`` environment variable or from the metadata
    server on Google Compute Engine.

    JSON key file: Specify 'Project Id', 'Keyfile Path' and 'Scope'.

    Legacy P12 key files are not supported.

    JSON data provided in the UI: Specify 'Keyfile JSON'.

    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    """

    def __init__(self, gcp_conn_id: str = 'google_cloud_default', delegate_to: str = None) -> None:
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.extras = self.get_connection(self.gcp_conn_id).extra_dejson  # type: Dict

    def _get_credentials(self) -> google.auth.credentials.Credentials:
        """
        Returns the Credentials object for Google API
        """
        key_path = self._get_field('key_path', None)  # type: Optional[str]
        keyfile_dict = self._get_field('keyfile_dict', None)  # type: Optional[str]

        if not key_path and not keyfile_dict:
            self.log.info('Getting connection using `google.auth.default()` '
                          'since no key file is defined for hook.')
            credentials, _ = google.auth.default(scopes=self.scopes)
        elif key_path:
            # Get credentials from a JSON file.
            if key_path.endswith('.json'):
                self.log.debug('Getting connection using JSON key file %s' % key_path)
                credentials = (
                    google.oauth2.service_account.Credentials.from_service_account_file(
                        key_path, scopes=self.scopes)
                )
            elif key_path.endswith('.p12'):
                raise AirflowException('Legacy P12 key file are not supported, '
                                       'use a JSON key file.')
            else:
                raise AirflowException('Unrecognised extension for key file.')
        else:
            # Get credentials from JSON data provided in the UI.
            try:
                assert keyfile_dict is not None
                keyfile_dict_json = json.loads(keyfile_dict)  # type: Dict[str, str]

                # Depending on how the JSON was formatted, it may contain
                # escaped newlines. Convert those to actual newlines.
                keyfile_dict_json['private_key'] = keyfile_dict_json['private_key'].replace(
                    '\\n', '\n')

                credentials = (
                    google.oauth2.service_account.Credentials.from_service_account_info(
                        keyfile_dict_json, scopes=self.scopes)
                )
            except json.decoder.JSONDecodeError:
                raise AirflowException('Invalid key JSON.')

        return credentials.with_subject(self.delegate_to) \
            if self.delegate_to else credentials

    def _get_access_token(self) -> str:
        """
        Returns a valid access token from Google API Credentials
        """
        return self._get_credentials().token

    def _authorize(self) -> google_auth_httplib2.AuthorizedHttp:
        """
        Returns an authorized HTTP object to be used to build a Google cloud
        service hook connection.
        """
        credentials = self._get_credentials()
        http = httplib2.Http()
        authed_http = google_auth_httplib2.AuthorizedHttp(
            credentials, http=http)
        return authed_http

    def _get_field(self, f: str, default: Any = None) -> Any:
        """
        Fetches a field from extras, and returns it. This is some Airflow
        magic. The google_cloud_platform hook type adds custom UI elements
        to the hook page, which allow admins to specify service_account,
        key_path, etc. They get formatted as shown below.
        """
        long_f = 'extra__google_cloud_platform__{}'.format(f)
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
        return self._get_field('project')

    @property
    def client_info(self) -> ClientInfo:
        """
        Return client information used to generate a user-agent for API calls.

        It allows for better errors tracking.

        This object is only used by the google-cloud-* libraries that are built specifically for
        the Google Cloud Platform. It is not supported by The Google APIs Python Client that use Discovery
        based APIs.
        """
        client_info = ClientInfo(client_library_version='airflow_v' + version.version)
        return client_info

    @property
    def scopes(self) -> Sequence[str]:
        """
        Return OAuth 2.0 scopes.

        :return: Returns the scope defined in the connection configuration, or the default scope
        :rtype: Sequence[str]
        """
        scope_value = self._get_field('scope', None)  # type: Optional[str]

        return [s.strip() for s in scope_value.split(',')] \
            if scope_value else _DEFAULT_SCOPES

    @staticmethod
    def catch_http_exception(func: Callable[..., RT]) -> Callable[..., RT]:
        """
        Function decorator that intercepts HTTP Errors and raises AirflowException
        with more informative message.
        """

        @functools.wraps(func)
        def wrapper_decorator(self: GoogleCloudBaseHook, *args, **kwargs) -> RT:
            try:
                return func(self, *args, **kwargs)
            except GoogleAPICallError as e:
                if isinstance(e, AlreadyExists):
                    raise e
                else:
                    self.log.error('The request failed:\n%s', str(e))
                    raise AirflowException(e)
            except RetryError as e:
                self.log.error('The request failed due to a retryable error and retry attempts failed.')
                raise AirflowException(e)
            except ValueError as e:
                self.log.error('The request failed, the parameters are invalid.')
                raise AirflowException(e)
            except HttpError as e:
                self.log.error('The request failed:\n%s', str(e))
                raise AirflowException(e)

        return wrapper_decorator

    @staticmethod
    def fallback_to_default_project_id(func: Callable[..., RT]) -> Callable[..., RT]:
        """
        Decorator that provides fallback for Google Cloud Platform project id. If
        the project is None it will be replaced with the project_id from the
        service account the Hook is authenticated with. Project id can be specified
        either via project_id kwarg or via first parameter in positional args.

        :param func: function to wrap
        :return: result of the function call
        """
        @functools.wraps(func)
        def inner_wrapper(self: GoogleCloudBaseHook, *args, **kwargs) -> RT:
            if args:
                raise AirflowException(
                    "You must use keyword arguments in this methods rather than"
                    " positional")
            if 'project_id' in kwargs:
                kwargs['project_id'] = kwargs['project_id'] or self.project_id
            else:
                kwargs['project_id'] = self.project_id
            if not kwargs['project_id']:
                raise AirflowException("The project id must be passed either as "
                                       "keyword project_id parameter or as project_id extra "
                                       "in GCP connection definition. Both are not set!")
            return func(self, *args, **kwargs)
        return inner_wrapper

    @staticmethod
    def provide_gcp_credential_file(func: Callable[..., RT]) -> Callable[..., RT]:
        """
        Function decorator that provides a ``GOOGLE_APPLICATION_CREDENTIALS``
        environment variable, pointing to file path of a JSON file of service
        account key.
        """
        @functools.wraps(func)
        def wrapper(self: GoogleCloudBaseHook, *args, **kwargs) -> RT:
            with tempfile.NamedTemporaryFile(mode='w+t') as conf_file:
                key_path = self._get_field('key_path', None)  # type: Optional[str]  # noqa: E501  #  pylint: disable=protected-access
                keyfile_dict = self._get_field('keyfile_dict', None)  # type: Optional[Dict]  # noqa: E501  # pylint: disable=protected-access
                if key_path:
                    if key_path.endswith('.p12'):
                        raise AirflowException('Legacy P12 key file are not supported, use a JSON key file.')
                    os.environ[CREDENTIALS] = key_path
                elif keyfile_dict:
                    conf_file.write(keyfile_dict)
                    conf_file.flush()
                    os.environ[CREDENTIALS] = conf_file.name
                return func(self, *args, **kwargs)
        return wrapper
