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

from typing import Any
from unittest import mock

import pytest

from airflow.providers.common.compat.sdk import XCom
from airflow.providers.google.cloud.links.base import BaseGoogleLink
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk.execution_time.comms import XComResult


TEST_LOCATION = "test-location"
TEST_CLUSTER_ID = "test-cluster-id"
TEST_PROJECT_ID = "test-project-id"
EXPECTED_GOOGLE_LINK_KEY = "google_link_for_test"
EXPECTED_GOOGLE_LINK_NAME = "Google Link for Test"
EXPECTED_GOOGLE_LINK_FORMAT = "/services/locations/{location}/clusters/{cluster_id}?project={project_id}"
EXPECTED_GOOGLE_LINK = "https://console.cloud.google.com" + EXPECTED_GOOGLE_LINK_FORMAT.format(
    location=TEST_LOCATION, cluster_id=TEST_CLUSTER_ID, project_id=TEST_PROJECT_ID
)


class GoogleLink(BaseGoogleLink):
    key = EXPECTED_GOOGLE_LINK_KEY
    name = EXPECTED_GOOGLE_LINK_NAME
    format_str = EXPECTED_GOOGLE_LINK_FORMAT


class TestBaseGoogleLink:
    def test_class_attributes(self):
        assert GoogleLink.key == EXPECTED_GOOGLE_LINK_KEY
        assert GoogleLink.name == EXPECTED_GOOGLE_LINK_NAME
        assert GoogleLink.format_str == EXPECTED_GOOGLE_LINK_FORMAT

    def test_persist(self):
        mock_context = mock.MagicMock()

        if AIRFLOW_V_3_0_PLUS:
            GoogleLink.persist(
                context=mock_context,
                location=TEST_LOCATION,
                cluster_id=TEST_CLUSTER_ID,
                project_id=TEST_PROJECT_ID,
            )
            mock_context["ti"].xcom_push.assert_called_once_with(
                key=EXPECTED_GOOGLE_LINK_KEY,
                value={
                    "location": TEST_LOCATION,
                    "cluster_id": TEST_CLUSTER_ID,
                    "project_id": TEST_PROJECT_ID,
                },
            )
        else:
            GoogleLink.persist(
                context=mock_context,
                location=TEST_LOCATION,
                cluster_id=TEST_CLUSTER_ID,
                project_id=TEST_PROJECT_ID,
            )


class MyOperator(GoogleCloudBaseOperator):
    operator_extra_links = (GoogleLink(),)

    def __init__(self, project_id: str, location: str, cluster_id: str, **kwargs):
        super().__init__(**kwargs)
        self.project_id = project_id
        self.location = location
        self.cluster_id = cluster_id

    @property
    def extra_links_params(self) -> dict[str, Any]:
        return {
            "project_id": self.project_id,
            "cluster_id": self.cluster_id,
            "location": self.location,
        }

    def execute(self, context) -> Any:
        GoogleLink.persist(context=context)


class TestOperatorWithBaseGoogleLink:
    @pytest.mark.db_test
    def test_get_link(self, create_task_instance_of_operator, session, mock_supervisor_comms):
        expected_url = EXPECTED_GOOGLE_LINK
        link = GoogleLink()
        ti = create_task_instance_of_operator(
            MyOperator,
            dag_id="test_link_dag",
            task_id="test_link_task",
            location=TEST_LOCATION,
            cluster_id=TEST_CLUSTER_ID,
            project_id=TEST_PROJECT_ID,
        )
        session.add(ti)
        session.commit()

        if AIRFLOW_V_3_0_PLUS and mock_supervisor_comms:
            mock_supervisor_comms.send.return_value = XComResult(
                key="key",
                value={
                    "cluster_id": ti.task.cluster_id,
                    "location": ti.task.location,
                    "project_id": ti.task.project_id,
                },
            )
        actual_url = link.get_link(operator=ti.task, ti_key=ti.key)
        assert actual_url == expected_url

    @pytest.mark.db_test
    @mock.patch.object(XCom, "get_value")
    def test_get_link_uses_xcom_url_and_skips_get_config(
        self,
        mock_get_value,
        create_task_instance_of_operator,
        session,
    ):
        xcom_url = "https://console.cloud.google.com/some/service?project=test-proj"
        mock_get_value.return_value = xcom_url

        link = GoogleLink()
        ti = create_task_instance_of_operator(
            MyOperator,
            dag_id="test_link_dag",
            task_id="test_link_task",
            location=TEST_LOCATION,
            cluster_id=TEST_CLUSTER_ID,
            project_id=TEST_PROJECT_ID,
        )
        session.add(ti)
        session.commit()

        with mock.patch.object(GoogleLink, "get_config", autospec=True) as m_get_config:
            actual_url = link.get_link(operator=ti.task, ti_key=ti.key)

        assert actual_url == xcom_url
        m_get_config.assert_not_called()

    @pytest.mark.db_test
    @mock.patch.object(XCom, "get_value")
    def test_get_link_falls_back_to_get_config_when_xcom_not_http(
        self,
        mock_get_value,
        create_task_instance_of_operator,
        session,
    ):
        mock_get_value.return_value = "gs://bucket/path"

        link = GoogleLink()
        ti = create_task_instance_of_operator(
            MyOperator,
            dag_id="test_link_dag",
            task_id="test_link_task",
            location=TEST_LOCATION,
            cluster_id=TEST_CLUSTER_ID,
            project_id=TEST_PROJECT_ID,
        )
        session.add(ti)
        session.commit()

        expected_formatted = "https://console.cloud.google.com/expected/link?project=test-proj"
        with (
            mock.patch.object(
                GoogleLink,
                "get_config",
                return_value={
                    "project_id": ti.task.project_id,
                    "location": ti.task.location,
                    "cluster_id": ti.task.cluster_id,
                },
            ) as m_get_config,
            mock.patch.object(GoogleLink, "_format_link", return_value=expected_formatted) as m_fmt,
        ):
            actual_url = link.get_link(operator=ti.task, ti_key=ti.key)

        assert actual_url == expected_formatted
        m_get_config.assert_called_once()
        m_fmt.assert_called_once()
