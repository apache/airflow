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
import json
import functools
import logging
import os
import tempfile
import httplib2

import google.auth
from google.auth.environment_vars import CREDENTIALS

import google_auth_httplib2
import google.oauth2.service_account
from google.api_core.exceptions import GoogleAPICallError, AlreadyExists, RetryError, Forbidden, \
    ResourceExhausted
from googleapiclient.errors import HttpError
import tenacity
from googleapiclient.http import set_user_agent

from airflow import LoggingMixin, version
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook

logger = LoggingMixin().log

_DEFAULT_SCOPES = ('https://www.googleapis.com/auth/cloud-platform',)


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


def is_soft_quota_exception(exception):
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
        return any(
            reason in error["reason"]
            for reason in INVALID_REASONS
            for error in exception.errors
        )

    if isinstance(exception, ResourceExhausted):
        return any(
            key in error.details()
            for key in INVALID_KEYS
            for error in exception.errors
        )

    return False


class retry_if_temporary_quota(tenacity.retry_if_exception):  # pylint: disable=invalid-name
    """Retries if there was an exception for exceeding the temporary quote limit."""

    def __init__(self):
        super(retry_if_temporary_quota, self).__init__(is_soft_quota_exception)


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

    def __init__(self, gcp_conn_id='google_cloud_default', delegate_to=None):
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.extras = self.get_connection(self.gcp_conn_id).extra_dejson

    def _get_credentials(self):
        """
        Returns the Credentials object for Google API
        """
        key_path = self._get_field('key_path', False)
        keyfile_dict = self._get_field('keyfile_dict', False)
        scope = self._get_field('scope', None)
        if scope:
            scopes = [s.strip() for s in scope.split(',')]
        else:
            scopes = _DEFAULT_SCOPES

        if not key_path and not keyfile_dict:
            self.log.info('Getting connection using `google.auth.default()` '
                          'since no key file is defined for hook.')
            credentials, _ = google.auth.default(scopes=scopes)
        elif key_path:
            # Get credentials from a JSON file.
            if key_path.endswith('.json'):
                self.log.debug('Getting connection using JSON key file %s' % key_path)
                credentials = (
                    google.oauth2.service_account.Credentials.from_service_account_file(
                        key_path, scopes=scopes)
                )
            elif key_path.endswith('.p12'):
                raise AirflowException('Legacy P12 key file are not supported, '
                                       'use a JSON key file.')
            else:
                raise AirflowException('Unrecognised extension for key file.')
        else:
            # Get credentials from JSON data provided in the UI.
            try:
                keyfile_dict = json.loads(keyfile_dict)

                # Depending on how the JSON was formatted, it may contain
                # escaped newlines. Convert those to actual newlines.
                keyfile_dict['private_key'] = keyfile_dict['private_key'].replace(
                    '\\n', '\n')

                credentials = (
                    google.oauth2.service_account.Credentials.from_service_account_info(
                        keyfile_dict, scopes=scopes)
                )
            except json.decoder.JSONDecodeError:
                raise AirflowException('Invalid key JSON.')

        return credentials.with_subject(self.delegate_to) \
            if self.delegate_to else credentials

    def _get_access_token(self):
        """
        Returns a valid access token from Google API Credentials
        """
        return self._get_credentials().token

    def _authorize(self):
        """
        Returns an authorized HTTP object to be used to build a Google cloud
        service hook connection.
        """
        credentials = self._get_credentials()
        http = httplib2.Http()
        http = set_user_agent(http, "airflow/" + version.version)
        authed_http = google_auth_httplib2.AuthorizedHttp(
            credentials, http=http)
        return authed_http

    def _get_field(self, f, default=None):
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
    def project_id(self):
        return self._get_field('project')

    @property
    def num_retries(self):
        """
        Returns num_retries from Connection.

        :return: the number of times each API request should be retried
        :rtype: int
        """
        return self._get_field('num_retries') or 5

    @staticmethod
    def quota_retry(*args, **kwargs):
        """
        Function decorator that provides a mechanism to repeat requests in response to exceeding a temporary
        quote limit.
        """
        def decorator(fun):
            default_kwargs = {
                'wait': tenacity.wait_exponential(multiplier=1, max=100),
                'retry': retry_if_temporary_quota(),
                'before': tenacity.before_log(logger, logging.DEBUG),
                'after': tenacity.after_log(logger, logging.DEBUG),
            }
            default_kwargs.update(**kwargs)
            return tenacity.retry(
                *args, **default_kwargs
            )(fun)
        return decorator

    @staticmethod
    def catch_http_exception(func):
        """
        Function decorator that intercepts HTTP Errors and raises AirflowException
        with more informative message.
        """

        @functools.wraps(func)
        def wrapper_decorator(self, *args, **kwargs):
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
    def fallback_to_default_project_id(func):
        """
        Decorator that provides fallback for Google Cloud Platform project id. If
        the project is None it will be replaced with the project_id from the
        service account the Hook is authenticated with. Project id can be specified
        either via project_id kwarg or via first parameter in positional args.

        :param func: function to wrap
        :return: result of the function call
        """
        @functools.wraps(func)
        def inner_wrapper(self, *args, **kwargs):
            if len(args) > 0:
                raise AirflowException(
                    "You must use keyword arguments in this methods rather than"
                    " positional")
            if 'project_id' in kwargs:
                kwargs['project_id'] = self._get_project_id(kwargs['project_id'])
            else:
                kwargs['project_id'] = self._get_project_id(None)
            if not kwargs['project_id']:
                raise AirflowException("The project id must be passed either as "
                                       "keyword project_id parameter or as project_id extra "
                                       "in GCP connection definition. Both are not set!")
            return func(self, *args, **kwargs)
        return inner_wrapper

    def _get_project_id(self, project_id):
        """
        In case project_id is None, overrides it with default project_id from
        the service account that is authorized.

        :param project_id: project id to
        :type project_id: str
        :return: the project_id specified or default project id if project_id is None
        """
        return project_id if project_id else self.project_id

    class _Decorators(object):
        """A private inner class for keeping all decorator methods."""

        @staticmethod
        def provide_gcp_credential_file(func):
            """
            Function decorator that provides a GOOGLE_APPLICATION_CREDENTIALS
            environment variable, pointing to file path of a JSON file of service
            account key.
            """
            @functools.wraps(func)
            def wrapper(self, *args, **kwargs):
                with tempfile.NamedTemporaryFile(mode='w+t') as conf_file:
                    key_path = self._get_field('key_path', False)
                    keyfile_dict = self._get_field('keyfile_dict', False)
                    current_env_state = os.environ.get(CREDENTIALS)
                    try:
                        if key_path:
                            if key_path.endswith('.p12'):
                                raise AirflowException(
                                    'Legacy P12 key file are not supported, use a JSON key file.'
                                )
                            os.environ[CREDENTIALS] = key_path
                        elif keyfile_dict:
                            conf_file.write(keyfile_dict)
                            conf_file.flush()
                            os.environ[CREDENTIALS] = conf_file.name
                        return func(self, *args, **kwargs)
                    finally:
                        if current_env_state is None:
                            if CREDENTIALS in os.environ:
                                del os.environ[CREDENTIALS]
                        else:
                            os.environ[CREDENTIALS] = current_env_state
            return wrapper
