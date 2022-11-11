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
"""This module contains a Google Calendar API hook"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Sequence

from googleapiclient.discovery import build

from airflow.exceptions import AirflowException
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook


class GoogleCalendarHook(GoogleBaseHook):
    """
    Interact with Google Calendar via Google Cloud connection
    Reading and writing cells in Google Sheet:
    https://developers.google.com/calendar/api/v3/reference

    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param api_version: API Version. For example v3
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

    def __init__(
        self,
        api_version: str,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            delegate_to=delegate_to,
            impersonation_chain=impersonation_chain,
        )
        self.gcp_conn_id = gcp_conn_id
        self.api_version = api_version
        self.delegate_to = delegate_to
        self._conn = None

    def get_conn(self) -> Any:
        """
        Retrieves connection to Google Calendar.

        :return: Google Calendar services object.
        :rtype: Any
        """
        if not self._conn:
            http_authorized = self._authorize()
            self._conn = build("calendar", self.api_version, http=http_authorized, cache_discovery=False)

        return self._conn

    def get_events(
        self,
        calendar_id: str = "primary",
        i_cal_uid: str | None = None,
        max_attendees: int | None = None,
        max_results: int | None = None,
        order_by: str | None = None,
        private_extended_property: str | None = None,
        q: str | None = None,
        shared_extended_property: str | None = None,
        show_deleted: bool | None = False,
        show_hidden_invitation: bool | None = False,
        single_events: bool | None = False,
        sync_token: str | None = None,
        time_max: datetime | None = None,
        time_min: datetime | None = None,
        time_zone: str | None = None,
        updated_min: datetime | None = None,
    ) -> list:
        """
        Gets events from Google Calendar from a single calendar_id
        https://developers.google.com/calendar/api/v3/reference/events/list

        :param calendar_id: The Google Calendar ID to interact with
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
        :param q: Optional. Free text search.
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
        :rtype: List
        """
        service = self.get_conn()
        page_token = None
        events = []
        while True:
            response = (
                service.events()
                .list(
                    calendarId=calendar_id,
                    iCalUID=i_cal_uid,
                    maxAttendees=max_attendees,
                    maxResults=max_results,
                    orderBy=order_by,
                    pageToken=page_token,
                    privateExtendedProperty=private_extended_property,
                    q=q,
                    sharedExtendedProperty=shared_extended_property,
                    showDeleted=show_deleted,
                    showHiddenInvitations=show_hidden_invitation,
                    singleEvents=single_events,
                    syncToken=sync_token,
                    timeMax=time_max,
                    timeMin=time_min,
                    timeZone=time_zone,
                    updatedMin=updated_min,
                )
                .execute(num_retries=self.num_retries)
            )
            events.extend(response["items"])
            page_token = response.get("nextPageToken")
            if not page_token:
                break
        return events

    def create_event(
        self,
        event: dict[str, Any],
        calendar_id: str = "primary",
        conference_data_version: int | None = 0,
        max_attendees: int | None = None,
        send_notifications: bool | None = False,
        send_updates: str | None = "false",
        supports_attachments: bool | None = False,
    ) -> dict:
        """
        Create event on the specified calendar
        https://developers.google.com/calendar/api/v3/reference/events/insert

        :param calendar_id: The Google Calendar ID to interact with
        :param conference_data_version: Optional. Version number of conference data
            supported by the API client.
        :param max_attendees: Optional. If there are more than the specified number of attendees,
            only the participant is returned.
        :param send_notifications: Optional. Default is False
        :param send_updates: Optional. Default is "false". Acceptable values as "all", "none",
            ``"externalOnly"``
            https://developers.google.com/calendar/api/v3/reference/events#resource
        :rtype: Dict
        """
        if "start" not in event or "end" not in event:
            raise AirflowException(
                f"start and end must be specified in the event body while creating an event. API docs:"
                f"https://developers.google.com/calendar/api/{self.api_version}/reference/events/insert "
            )
        service = self.get_conn()

        response = (
            service.events()
            .insert(
                calendarId=calendar_id,
                conferenceDataVersion=conference_data_version,
                maxAttendees=max_attendees,
                sendNotifications=send_notifications,
                sendUpdates=send_updates,
                supportsAttachments=supports_attachments,
                body=event,
            )
            .execute(num_retries=self.num_retries)
        )

        return response
