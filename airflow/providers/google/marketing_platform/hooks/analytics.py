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
from typing import Any, Dict, List, Optional

from googleapiclient.discovery import Resource, build
from googleapiclient.http import MediaFileUpload

from airflow.providers.google.cloud.hooks.base import CloudBaseHook


class GoogleAnalyticsHook(CloudBaseHook):
    """
    Hook for Google Analytics 360.
    """

    def __init__(
        self,
        api_version: str = "v3",
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.api_version = api_version
        self.gcp_connection_is = gcp_conn_id
        self._conn = None

    def _paginate(self, resource: Resource, list_args: Optional[Dict[str, Any]] = None):
        list_args = list_args or {}
        result: List[Dict] = []
        while True:
            # start index has value 1
            request = resource.list(start_index=len(result) + 1, **list_args)  # pylint: disable=no-member
            response = request.execute(num_retries=self.num_retries)
            result.extend(response.get("items", []))
            # result is the number of fetched links from Analytics
            # when all links will be added to the result
            # the loop will break
            if response["totalResults"] <= len(result):
                break
        return result

    def get_conn(self) -> Resource:
        """
        Retrieves connection to Google Analytics 360.
        """
        if not self._conn:
            http_authorized = self._authorize()
            self._conn = build(
                "analytics",
                self.api_version,
                http=http_authorized,
                cache_discovery=False,
            )
        return self._conn

    def list_accounts(self) -> List[Dict[str, Any]]:
        """
        Lists accounts list from Google Analytics 360.
        """

        self.log.info("Retrieving accounts list...")
        conn = self.get_conn()
        accounts = conn.management().accounts()  # pylint: disable=no-member
        result = self._paginate(accounts)
        return result

    def get_ad_words_link(
        self, account_id: str, web_property_id: str, web_property_ad_words_link_id: str
    ) -> Dict[str, Any]:
        """
        Returns a web property-Google Ads link to which the user has access.

        :param account_id: ID of the account which the given web property belongs to.
        :type account_id: string
        :param web_property_id: Web property-Google Ads link UA-string.
        :type web_property_id: string
        :param web_property_ad_words_link_id: to retrieve the Google Ads link for.
        :type web_property_ad_words_link_id: string

        :returns: web property-Google Ads
        :rtype: Dict
        """

        self.log.info("Retrieving ad words links...")
        ad_words_link = (
            self.get_conn()  # pylint: disable=no-member
            .management()
            .webPropertyAdWordsLinks()
            .get(
                accountId=account_id,
                webPropertyId=web_property_id,
                webPropertyAdWordsLinkId=web_property_ad_words_link_id,
            )
            .execute(num_retries=self.num_retries)
        )
        return ad_words_link

    def list_ad_words_links(
        self, account_id: str, web_property_id: str
    ) -> List[Dict[str, Any]]:
        """
        Lists webProperty-Google Ads links for a given web property.

        :param account_id: ID of the account which the given web property belongs to.
        :type account_id: str
        :param web_property_id: Web property UA-string to retrieve the Google Ads links for.
        :type web_property_id: str

        :returns: list of entity Google Ads links.
        :rtype: list
        """

        self.log.info("Retrieving ad words list...")
        conn = self.get_conn()
        ads_links = (
            conn.management().webPropertyAdWordsLinks()  # pylint: disable=no-member
        )
        list_args = {"accountId": account_id, "webPropertyId": web_property_id}
        result = self._paginate(ads_links, list_args)
        return result

    def upload_data(
        self,
        file_location: str,
        account_id: str,
        web_property_id: str,
        custom_data_source_id: str,
        resumable_upload: bool = False,
    ) -> None:

        """
        Uploads file to GA via the Data Import API

        :param file_location: The path and name of the file to upload.
        :type file_location: str
        :param account_id: The GA account Id to which the data upload belongs.
        :type account_id: str
        :param web_property_id: UA-string associated with the upload.
        :type web_property_id: str
        :param custom_data_source_id: Custom Data Source Id to which this data import belongs.
        :type custom_data_source_id: str
        :param resumable_upload: flag to upload the file in a resumable fashion, using a
            series of at least two requests.
        :type resumable_upload: bool
        """

        media = MediaFileUpload(
            file_location,
            mimetype="application/octet-stream",
            resumable=resumable_upload,
        )

        self.log.info(
            "Uploading file to GA file for accountId: %s, webPropertyId:%s and customDataSourceId:%s ",
            account_id,
            web_property_id,
            custom_data_source_id,
        )

        self.get_conn().management().uploads().uploadData(  # pylint: disable=no-member
            accountId=account_id,
            webPropertyId=web_property_id,
            customDataSourceId=custom_data_source_id,
            media_body=media,
        ).execute()

    def delete_upload_data(
        self,
        account_id: str,
        web_property_id: str,
        custom_data_source_id: str,
        delete_request_body: Dict[str, Any],
    ) -> None:
        """
        Deletes the uploaded data for a given account/property/dataset

        :param account_id: The GA account Id to which the data upload belongs.
        :type account_id: str
        :param web_property_id: UA-string associated with the upload.
        :type web_property_id: str
        :param custom_data_source_id: Custom Data Source Id to which this data import belongs.
        :type custom_data_source_id: str
        :param delete_request_body: Dict of customDataImportUids to delete.
        :type delete_request_body: dict
        """

        self.log.info(
            "Deleting previous uploads to GA file for accountId:%s, "
            "webPropertyId:%s and customDataSourceId:%s ",
            account_id,
            web_property_id,
            custom_data_source_id,
        )

        self.get_conn().management().uploads().deleteUploadData(  # pylint: disable=no-member
            accountId=account_id,
            webPropertyId=web_property_id,
            customDataSourceId=custom_data_source_id,
            body=delete_request_body,
        ).execute()

    def list_uploads(
        self, account_id, web_property_id, custom_data_source_id
    ) -> List[Dict[str, Any]]:
        """
        Get list of data upload from GA

        :param account_id: The GA account Id to which the data upload belongs.
        :type account_id: str
        :param web_property_id: UA-string associated with the upload.
        :type web_property_id: str
        :param custom_data_source_id: Custom Data Source Id to which this data import belongs.
        :type custom_data_source_id: str
        """
        self.log.info(
            "Getting list of uploads for accountId:%s, webPropertyId:%s and customDataSourceId:%s ",
            account_id,
            web_property_id,
            custom_data_source_id,
        )

        uploads = self.get_conn().management().uploads()  # pylint: disable=no-member
        list_args = {
            "accountId": account_id,
            "webPropertyId": web_property_id,
            "customDataSourceId": custom_data_source_id,
        }
        result = self._paginate(uploads, list_args)
        return result
