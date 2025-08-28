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

from unittest.mock import Mock, patch

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.tableau.sensors.tableau import (
    TableauJobFinishCode,
    TableauJobStatusSensor,
)


class TestTableauJobStatusSensor:
    """
    Test Class for JobStatusSensor
    """

    def setup_method(self):
        self.kwargs = {"job_id": "job_2", "site_id": "test_site", "task_id": "task", "dag": None}

    @patch("airflow.providers.tableau.sensors.tableau.TableauHook")
    def test_poke(self, mock_tableau_hook):
        """
        Test poke when job is successful.
        """
        mock_tableau_hook.return_value.__enter__ = Mock(return_value=mock_tableau_hook)
        # Mock the get_job_details method to return a dictionary
        mock_tableau_hook.get_job_details.return_value = {
            "finish_code": TableauJobFinishCode.SUCCESS,
            "job_type": "RefreshExtract",
            "object_name": "Test Workbook",
            "object_id": "workbook_id_123",
        }
        sensor = TableauJobStatusSensor(**self.kwargs)

        job_finished = sensor.poke(context={})

        assert job_finished
        mock_tableau_hook.get_job_details.assert_called_once_with(job_id=sensor.job_id)

    @pytest.mark.parametrize(
        "finish_code",
        [
            pytest.param(TableauJobFinishCode.ERROR, id="ERROR"),
            pytest.param(TableauJobFinishCode.CANCELED, id="CANCELED"),
        ],
    )
    @patch("airflow.providers.tableau.sensors.tableau.TableauHook")
    def test_poke_failed(self, mock_tableau_hook, finish_code):
        """
        Test poke when job fails or is canceled.
        """
        mock_tableau_hook.return_value.__enter__ = Mock(return_value=mock_tableau_hook)
        # Mock the get_job_details method to return a dictionary with the specific finish_code
        mock_tableau_hook.get_job_details.return_value = {
            "finish_code": finish_code,
            "job_type": "RefreshExtract",
            "object_name": "Failed Datasource",
            "object_id": "datasource_id_456",
        }
        sensor = TableauJobStatusSensor(**self.kwargs)

        with pytest.raises(AirflowException):
            sensor.poke({})
        # Optionally, assert the exception message if needed, though the original test did not.
        # For example: assert "The Tableau Refresh Datasource Job for 'Failed Datasource' failed!" in str(excinfo.value)
        mock_tableau_hook.get_job_details.assert_called_once_with(job_id=sensor.job_id)
