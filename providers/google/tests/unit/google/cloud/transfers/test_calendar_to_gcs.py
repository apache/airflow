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

import warnings
from unittest import mock

import pytest

from airflow.providers.google.cloud.transfers.calendar_to_gcs import GoogleCalendarToGCSOperator

API_VERSION = "v3"
CALENDAR_ID = "calendar_id"
EVENT = {"id": "123"}
BUCKET = "destination_bucket"
PATH = "path/to/reports"
GCP_CONN_ID = "test"
IMPERSONATION_CHAIN = ["account1", "account2"]


class TestGoogleCalendarToGCSOperator:
    @mock.patch("airflow.providers.google.cloud.transfers.calendar_to_gcs.NamedTemporaryFile")
    @mock.patch("airflow.providers.google.cloud.transfers.calendar_to_gcs.json")
    @mock.patch("airflow.providers.google.cloud.transfers.calendar_to_gcs.GCSHook")
    @mock.patch("airflow.providers.google.cloud.transfers.calendar_to_gcs.GoogleCalendarHook")
    def test_upload_data(self, mock_calendar_hook, mock_gcs_hook, mock_json, mock_temp_file):
        expected_dest_file = f"{CALENDAR_ID}.json".replace(" ", "_")
        expected_dest_file = f"{PATH.strip('/')}/{expected_dest_file}"
        expected_gcs_uri = f"gs://{BUCKET}/{expected_dest_file}"

        mock_calendar_hook.return_value.get_events.return_value = [EVENT]
        mock_gcs_hook.return_value.upload.return_value = None
        file_handle = mock_temp_file.return_value.__enter__.return_value
        file_handle.name = "file_name"

        op = GoogleCalendarToGCSOperator(
            api_version=API_VERSION,
            task_id="test_task",
            calendar_id=CALENDAR_ID,
            destination_bucket=BUCKET,
            destination_path=PATH,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        result = op._upload_data(
            events=[EVENT],
        )

        # Test writing to file
        file_handle.flush.assert_called_once_with()

        # Test GCS Hook
        mock_gcs_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        # Assert tuple of (dest_file_name, gcs_uri) is returned
        assert result == (expected_dest_file, expected_gcs_uri)

    @mock.patch("airflow.providers.google.cloud.transfers.calendar_to_gcs.GoogleCalendarHook")
    @mock.patch(
        "airflow.providers.google.cloud.transfers.calendar_to_gcs.GoogleCalendarToGCSOperator._upload_data"
    )
    def test_execute_with_unwrap_single_true(self, mock_upload_data, mock_calendar_hook):
        context = {}
        data = [EVENT]
        expected_dest_file_name = f"{PATH.strip('/')}/{CALENDAR_ID}.json"
        mock_upload_data.return_value = (expected_dest_file_name, f"gs://{BUCKET}/{PATH}")
        mock_calendar_hook.return_value.get_events.return_value = data

        op = GoogleCalendarToGCSOperator(
            api_version=API_VERSION,
            task_id="test_task",
            calendar_id=CALENDAR_ID,
            destination_bucket=BUCKET,
            destination_path=PATH,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            unwrap_single=True,
        )
        result = op.execute(context)

        mock_calendar_hook.assert_called_once_with(
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        mock_calendar_hook.return_value.get_events.assert_called_once_with(
            calendar_id=CALENDAR_ID,
            i_cal_uid=None,
            max_attendees=None,
            max_results=None,
            order_by=None,
            private_extended_property=None,
            q=None,
            shared_extended_property=None,
            show_deleted=None,
            show_hidden_invitation=None,
            single_events=None,
            sync_token=None,
            time_max=None,
            time_min=None,
            time_zone=None,
            updated_min=None,
        )

        mock_upload_data.assert_called_once_with(data)
        # Assert dest_file_name is returned when unwrap_single=True and return_gcs_uri=False (default)
        assert result == expected_dest_file_name

    @mock.patch("airflow.providers.google.cloud.transfers.calendar_to_gcs.GoogleCalendarHook")
    @mock.patch(
        "airflow.providers.google.cloud.transfers.calendar_to_gcs.GoogleCalendarToGCSOperator._upload_data"
    )
    def test_execute_with_unwrap_single_false(self, mock_upload_data, mock_calendar_hook):
        context = {}
        data = [EVENT]
        expected_dest_file_name = f"{PATH.strip('/')}/{CALENDAR_ID}.json"
        expected_gcs_uri = f"gs://{BUCKET}/{expected_dest_file_name}"
        mock_upload_data.return_value = (expected_dest_file_name, expected_gcs_uri)
        mock_calendar_hook.return_value.get_events.return_value = data

        op = GoogleCalendarToGCSOperator(
            api_version=API_VERSION,
            task_id="test_task",
            calendar_id=CALENDAR_ID,
            destination_bucket=BUCKET,
            destination_path=PATH,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            unwrap_single=False,
            return_gcs_uri=True,
        )
        result = op.execute(context)

        mock_calendar_hook.assert_called_once_with(
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        mock_calendar_hook.return_value.get_events.assert_called_once_with(
            calendar_id=CALENDAR_ID,
            i_cal_uid=None,
            max_attendees=None,
            max_results=None,
            order_by=None,
            private_extended_property=None,
            q=None,
            shared_extended_property=None,
            show_deleted=None,
            show_hidden_invitation=None,
            single_events=None,
            sync_token=None,
            time_max=None,
            time_min=None,
            time_zone=None,
            updated_min=None,
        )

        mock_upload_data.assert_called_once_with(data)
        # Assert list of GCS URIs is returned when unwrap_single=False and return_gcs_uri=True
        assert result == [expected_gcs_uri]

    @mock.patch("airflow.providers.google.cloud.transfers.calendar_to_gcs.GoogleCalendarHook")
    @mock.patch(
        "airflow.providers.google.cloud.transfers.calendar_to_gcs.GoogleCalendarToGCSOperator._upload_data"
    )
    def test_execute_with_return_gcs_uri_true_and_unwrap_single_false(
        self, mock_upload_data, mock_calendar_hook
    ):
        context = {}
        data = [EVENT]
        expected_dest_file_name = f"{PATH.strip('/')}/{CALENDAR_ID}.json"
        expected_gcs_uri = f"gs://{BUCKET}/{expected_dest_file_name}"
        mock_upload_data.return_value = (expected_dest_file_name, expected_gcs_uri)
        mock_calendar_hook.return_value.get_events.return_value = data

        op = GoogleCalendarToGCSOperator(
            api_version=API_VERSION,
            task_id="test_task",
            calendar_id=CALENDAR_ID,
            destination_bucket=BUCKET,
            destination_path=PATH,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            unwrap_single=False,
            return_gcs_uri=True,
        )
        result = op.execute(context)

        mock_calendar_hook.assert_called_once_with(
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        mock_calendar_hook.return_value.get_events.assert_called_once_with(
            calendar_id=CALENDAR_ID,
            i_cal_uid=None,
            max_attendees=None,
            max_results=None,
            order_by=None,
            private_extended_property=None,
            q=None,
            shared_extended_property=None,
            show_deleted=None,
            show_hidden_invitation=None,
            single_events=None,
            sync_token=None,
            time_max=None,
            time_min=None,
            time_zone=None,
            updated_min=None,
        )

        mock_upload_data.assert_called_once_with(data)
        # Assert list of GCS URIs is returned when return_gcs_uri=True
        assert result == [expected_gcs_uri]

    def test_execute_with_return_gcs_uri_false_and_unwrap_single_false_raises_error(self):
        """Test that return_gcs_uri=False together with unwrap_single=False raises ValueError."""
        with pytest.raises(
            ValueError, match="return_gcs_uri cannot be False together with unwrap_single=False"
        ):
            GoogleCalendarToGCSOperator(
                api_version=API_VERSION,
                task_id="test_task",
                calendar_id=CALENDAR_ID,
                destination_bucket=BUCKET,
                destination_path=PATH,
                gcp_conn_id=GCP_CONN_ID,
                impersonation_chain=IMPERSONATION_CHAIN,
                unwrap_single=False,
                return_gcs_uri=False,
            )

    def test_unwrap_single_none_emits_warning(self):
        """Test that unwrap_single=None emits a deprecation warning."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            GoogleCalendarToGCSOperator(
                api_version=API_VERSION,
                task_id="test_task",
                calendar_id=CALENDAR_ID,
                destination_bucket=BUCKET,
                destination_path=PATH,
                gcp_conn_id=GCP_CONN_ID,
                impersonation_chain=IMPERSONATION_CHAIN,
                unwrap_single=None,
                return_gcs_uri=True,
            )
            assert len(w) == 1
            assert issubclass(w[0].category, FutureWarning)
            assert "unwrap_single will change from True to False" in str(w[0].message)

    def test_return_gcs_uri_none_emits_warning(self):
        """Test that return_gcs_uri=None emits a deprecation warning."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            GoogleCalendarToGCSOperator(
                api_version=API_VERSION,
                task_id="test_task",
                calendar_id=CALENDAR_ID,
                destination_bucket=BUCKET,
                destination_path=PATH,
                gcp_conn_id=GCP_CONN_ID,
                impersonation_chain=IMPERSONATION_CHAIN,
                unwrap_single=True,
                return_gcs_uri=None,
            )
            assert len(w) == 1
            assert issubclass(w[0].category, FutureWarning)
            assert "return_gcs_uri parameter is deprecated" in str(w[0].message)

    def test_both_none_emits_both_warnings(self):
        """Test that both None values emit both deprecation warnings."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            # Create operator but don't need to use it - just checking for warnings
            GoogleCalendarToGCSOperator(
                api_version=API_VERSION,
                task_id="test_task",
                calendar_id=CALENDAR_ID,
                destination_bucket=BUCKET,
                destination_path=PATH,
                gcp_conn_id=GCP_CONN_ID,
                impersonation_chain=IMPERSONATION_CHAIN,
                unwrap_single=None,
                return_gcs_uri=None,
            )
            # Should have 2 warnings and not raise ValueError since return_gcs_uri gets set to False
            # when None, and unwrap_single gets set to True when None
            assert len(w) == 2
            assert all(issubclass(warning.category, FutureWarning) for warning in w)

    def test_return_gcs_uri_true_with_unwrap_single_true_is_valid(self):
        """Test that return_gcs_uri=True with unwrap_single=True is valid (opposite of old logic)."""
        # This should NOT raise an error (the old test was checking the wrong condition)
        op = GoogleCalendarToGCSOperator(
            api_version=API_VERSION,
            task_id="test_task",
            calendar_id=CALENDAR_ID,
            destination_bucket=BUCKET,
            destination_path=PATH,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            unwrap_single=True,
            return_gcs_uri=True,
        )
        assert op.unwrap_single is True
        assert op.return_gcs_uri is True
