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
from __future__ import annotations

"""
Unit Tests for the Google Calendar Hook
"""

import unittest
from unittest import mock

from airflow.providers.google.suite.hooks.calendar import GoogleCalendarHook
from tests.providers.google.cloud.utils.base_gcp_mock import mock_base_gcp_hook_default_project_id

API_VERSION = 'api_version'
GCP_CONN_ID = 'test'
CALENDAR_ID = 'test12345'
EVENT = {
    'summary': 'Calendar Test Event',
    'description': 'A chance to test creating an event from airflow.',
    'start': {
        'dateTime': '2021-12-28T09:00:00-07:00',
        'timeZone': 'America/Los_Angeles',
    },
    'end': {
        'dateTime': '2021-12-28T17:00:00-07:00',
        'timeZone': 'America/Los_Angeles',
    },
}
NUM_RETRIES = 5
API_RESPONSE = {'test': 'response'}


class TestGoogleCalendarHook(unittest.TestCase):
    def setUp(self):
        with mock.patch(
            'airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__',
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.hook = GoogleCalendarHook(api_version=API_VERSION, gcp_conn_id=GCP_CONN_ID)

    @mock.patch("airflow.providers.google.suite.hooks.calendar.GoogleCalendarHook.get_conn")
    def test_get_events(self, get_conn):
        get_method = get_conn.return_value.events.return_value.list
        execute_method = get_method.return_value.execute
        execute_method.return_value = {"kind": "calendar#events", "nextPageToken": None, "items": [EVENT]}
        result = self.hook.get_events(calendar_id=CALENDAR_ID)
        self.assertEqual(result, [EVENT])
        execute_method.assert_called_once_with(num_retries=NUM_RETRIES)
        get_method.assert_called_once_with(
            calendarId=CALENDAR_ID,
            iCalUID=None,
            maxAttendees=None,
            maxResults=None,
            orderBy=None,
            pageToken=None,
            privateExtendedProperty=None,
            q=None,
            sharedExtendedProperty=None,
            showDeleted=False,
            showHiddenInvitations=False,
            singleEvents=False,
            syncToken=None,
            timeMax=None,
            timeMin=None,
            timeZone=None,
            updatedMin=None,
        )

    @mock.patch("airflow.providers.google.suite.hooks.calendar.GoogleCalendarHook.get_conn")
    def test_create_event(self, mock_get_conn):
        create_mock = mock_get_conn.return_value.events.return_value.insert
        create_mock.return_value.execute.return_value = API_RESPONSE

        result = self.hook.create_event(calendar_id=CALENDAR_ID, event=EVENT)

        create_mock.assert_called_once_with(
            body=EVENT,
            calendarId=CALENDAR_ID,
            conferenceDataVersion=0,
            maxAttendees=None,
            sendNotifications=False,
            sendUpdates='false',
            supportsAttachments=False,
        )
        assert result == API_RESPONSE
