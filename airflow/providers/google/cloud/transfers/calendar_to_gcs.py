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
from __future__ import annotations

import json
from datetime import datetime
from tempfile import NamedTemporaryFile
from typing import Any, Sequence

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.suite.hooks.calendar import GoogleCalendarHook


class GoogleCalendarToGCSOperator(BaseOperator):
    """
    Writes Google Calendar data into Google Cloud Storage.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleCalendarToGCSOperator`

    :param calendar_id: The Google Calendar ID to interact with.
    :param i_cal_uid: Optional. Specifies event ID in the ``iCalendar`` format in the response.
    :param max_attendees: Optional. If there are more than the specified number of attendees,
        only the participant is returned.
    :param max_results: Optional. Maximum number of events returned on one result page.
        Incomplete pages can be detected by a non-empty ``nextPageToken`` field in the response.
        By default the value is 250 events. The page size can never be larger than 2500 events
    :param order_by: Optional. Acceptable values are ``"startTime"`` or "updated"
    :param private_extended_property: Optional. Extended properties constraint specified as
        ``propertyName=value``. Matches only private properties. This parameter might be repeated
        multiple times to return events that match all given constraints.
    :param text_search_query: Optional. Free text search.
    :param shared_extended_property: Optional. Extended properties constraint specified as
        ``propertyName=value``. Matches only shared properties. This parameter might be repeated
        multiple times to return events that match all given constraints.
    :param show_deleted: Optional. False by default
    :param show_hidden_invitation: Optional. False by default
    :param single_events: Optional. False by default
    :param sync_token: Optional. Token obtained from the ``nextSyncToken`` field returned
    :param time_max: Optional. Upper bound (exclusive) for an event's start time to filter by.
        Default is no filter
    :param time_min: Optional. Lower bound (exclusive) for an event's end time to filter by.
        Default is no filter
    :param time_zone: Optional. Time zone used in response. Default is calendars time zone.
    :param updated_min: Optional. Lower bound for an event's last modification time
    :param destination_bucket: The destination Google Cloud Storage bucket where the
        report should be written to. (templated)
    :param destination_path: The Google Cloud Storage URI array for the object created by the operator.
        For example: ``path/to/my/files``.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields = [
        "calendar_id",
        "destination_bucket",
        "destination_path",
        "impersonation_chain",
    ]

    def __init__(
        self,
        *,
        destination_bucket: str,
        api_version: str,
        calendar_id: str = "primary",
        i_cal_uid: str | None = None,
        max_attendees: int | None = None,
        max_results: int | None = None,
        order_by: str | None = None,
        private_extended_property: str | None = None,
        text_search_query: str | None = None,
        shared_extended_property: str | None = None,
        show_deleted: bool | None = None,
        show_hidden_invitation: bool | None = None,
        single_events: bool | None = None,
        sync_token: str | None = None,
        time_max: datetime | None = None,
        time_min: datetime | None = None,
        time_zone: str | None = None,
        updated_min: datetime | None = None,
        destination_path: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.calendar_id = calendar_id
        self.api_version = api_version
        self.i_cal_uid = i_cal_uid
        self.max_attendees = max_attendees
        self.max_results = max_results
        self.order_by = order_by
        self.private_extended_property = private_extended_property
        self.text_search_query = text_search_query
        self.shared_extended_property = shared_extended_property
        self.show_deleted = show_deleted
        self.show_hidden_invitation = show_hidden_invitation
        self.single_events = single_events
        self.sync_token = sync_token
        self.time_max = time_max
        self.time_min = time_min
        self.time_zone = time_zone
        self.updated_min = updated_min
        self.destination_bucket = destination_bucket
        self.destination_path = destination_path
        self.impersonation_chain = impersonation_chain

    def _upload_data(
        self,
        events: list[Any],
    ) -> str:
        gcs_hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

        # Construct destination file path
        file_name = f"{self.calendar_id}.json".replace(" ", "_")
        dest_file_name = (
            f"{self.destination_path.strip('/')}/{file_name}" if self.destination_path else file_name
        )

        with NamedTemporaryFile("w+") as temp_file:
            # Write data
            json.dump(events, temp_file)
            temp_file.flush()

            # Upload to GCS
            gcs_hook.upload(
                bucket_name=self.destination_bucket,
                object_name=dest_file_name,
                filename=temp_file.name,
            )
        return dest_file_name

    def execute(self, context):
        calendar_hook = GoogleCalendarHook(
            api_version=self.api_version,
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

        events = calendar_hook.get_events(
            calendar_id=self.calendar_id,
            i_cal_uid=self.i_cal_uid,
            max_attendees=self.max_attendees,
            max_results=self.max_results,
            order_by=self.order_by,
            private_extended_property=self.private_extended_property,
            q=self.text_search_query,
            shared_extended_property=self.shared_extended_property,
            show_deleted=self.show_deleted,
            show_hidden_invitation=self.show_hidden_invitation,
            single_events=self.single_events,
            sync_token=self.sync_token,
            time_max=self.time_max,
            time_min=self.time_min,
            time_zone=self.time_zone,
            updated_min=self.updated_min,
        )
        gcs_path_to_file = self._upload_data(events)

        return gcs_path_to_file
