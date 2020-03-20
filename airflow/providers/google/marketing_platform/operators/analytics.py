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
"""
This module contains Google Analytics 360 operators.
"""

from airflow.models import BaseOperator
from airflow.providers.google.marketing_platform.hooks.analytics import GoogleAnalyticsHook
from airflow.utils.decorators import apply_defaults


class GoogleAnalyticsListAccountsOperator(BaseOperator):
    """
    Lists all accounts to which the user has access.

    .. seealso::
        Check official API docs:
        https://developers.google.com/analytics/devguides/config/mgmt/v3/mgmtReference/management/accounts/list
        and for python client
        http://googleapis.github.io/google-api-python-client/docs/dyn/analytics_v3.management.accounts.html#list

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleAnalyticsListAccountsOperator`

    :param api_version: The version of the api that will be requested for example 'v3'.
    :type api_version: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    """

    template_fields = (
        "api_version",
        "gcp_connection_id",
    )

    @apply_defaults
    def __init__(
        self,
        api_version: str = "v3",
        gcp_connection_id: str = "google_cloud_default",
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)

        self.api_version = api_version
        self.gcp_connection_id = gcp_connection_id

    def execute(self, context):
        hook = GoogleAnalyticsHook(
            api_version=self.api_version, gcp_connection_id=self.gcp_connection_id
        )
        result = hook.list_accounts()
        return result


class GoogleAnalyticsRetrieveAdsLinksListOperator(BaseOperator):
    """
    Lists webProperty-Google Ads links for a given web property

    .. seealso::
        Check official API docs:
        https://developers.google.com/analytics/devguides/config/mgmt/v3/mgmtReference/management/webPropertyAdWordsLinks/list#http-request

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleAnalyticsListAccountsOperator`

    :param account_id: ID of the account which the given web property belongs to.
    :type account_id: str
    :param web_property_id: Web property UA-string to retrieve the Google Ads links for.
    :type web_property_id: str
    """

    template_fields = (
        "api_version",
        "gcp_connection_id",
        "account_id",
        "web_property_id",
    )

    @apply_defaults
    def __init__(
        self,
        account_id: str,
        web_property_id: str,
        api_version: str = "v3",
        gcp_connection_id: str = "google_cloud_default",
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)

        self.account_id = account_id
        self.web_property_id = web_property_id
        self.api_version = api_version
        self.gcp_connection_id = gcp_connection_id

    def execute(self, context):
        hook = GoogleAnalyticsHook(
            api_version=self.api_version, gcp_connection_id=self.gcp_connection_id
        )
        result = hook.list_ad_words_links(
            account_id=self.account_id, web_property_id=self.web_property_id,
        )
        return result
