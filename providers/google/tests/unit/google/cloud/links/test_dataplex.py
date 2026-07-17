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

import pytest

from airflow.providers.google.cloud.links.dataplex import (
    DataplexCatalogAspectTypeLink,
    DataplexCatalogAspectTypesLink,
    DataplexCatalogEntryGroupLink,
    DataplexCatalogEntryGroupsLink,
    DataplexCatalogEntryLink,
    DataplexCatalogEntryTypeLink,
    DataplexCatalogEntryTypesLink,
    DataplexLakeLink,
    DataplexTaskLink,
    DataplexTasksLink,
)
from airflow.providers.google.cloud.operators.dataplex import (
    DataplexCatalogCreateAspectTypeOperator,
    DataplexCatalogCreateEntryGroupOperator,
    DataplexCatalogCreateEntryTypeOperator,
    DataplexCatalogGetAspectTypeOperator,
    DataplexCatalogGetEntryGroupOperator,
    DataplexCatalogGetEntryOperator,
    DataplexCatalogGetEntryTypeOperator,
    DataplexCreateLakeOperator,
    DataplexCreateTaskOperator,
    DataplexListTasksOperator,
)

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk.execution_time.comms import XComResult

TEST_LOCATION = "test-location"
TEST_PROJECT_ID = "test-project-id"
TEST_ENTRY_ID = "test-entry-id"
TEST_ENTRY_GROUP_ID = "test-entry-group-id"
TEST_ENTRY_GROUP_ID_BODY = {"description": "some description"}
TEST_ENTRY_GROUPS_ID = "test-entry-groups-id"
TEST_ENTRY_TYPE_ID = "test-entry-type-id"
TEST_ENTRY_TYPE_ID_BODY = {"description": "some description"}
TEST_ENTRY_TYPES_ID = "test-entry-types-id"
TEST_ASPECT_TYPE_ID = "test-aspect-type-id"
TEST_ASPECT_TYPE_ID_BODY = {"description": "some description"}
TEST_ASPECT_TYPES_ID = "test-aspect-types-id"
TEST_TASK_ID = "test-task-id"
TEST_TASKS_ID = "test-tasks-id"
TEST_LAKE_ID = "test-lake-id"
TEST_LAKE_BODY = {"name": "some_name"}

DATAPLEX_BASE_LINK = "https://console.cloud.google.com/dataplex/"
EXPECTED_DATAPLEX_CATALOG_ENTRY_GROUP_LINK = (
    DATAPLEX_BASE_LINK
    + f"projects/{TEST_PROJECT_ID}/locations/{TEST_LOCATION}/entryGroups/{TEST_ENTRY_GROUP_ID}?project={TEST_PROJECT_ID}"
)
EXPECTED_DATAPLEX_CATALOG_ENTRY_GROUPS_LINK = (
    DATAPLEX_BASE_LINK + f"catalog/entry-groups?project={TEST_PROJECT_ID}"
)
EXPECTED_DATAPLEX_CATALOG_ENTRY_TYPE_LINK = (
    DATAPLEX_BASE_LINK
    + f"projects/{TEST_PROJECT_ID}/locations/{TEST_LOCATION}/entryTypes/{TEST_ENTRY_TYPE_ID}?project={TEST_PROJECT_ID}"
)
EXPECTED_DATAPLEX_CATALOG_ENTRY_TYPES_LINK = (
    DATAPLEX_BASE_LINK + f"catalog/entry-types?project={TEST_PROJECT_ID}"
)
DATAPLEX_LAKE_LINK = (
    DATAPLEX_BASE_LINK + f"lakes/{TEST_LAKE_ID};location={TEST_LOCATION}?project={TEST_PROJECT_ID}"
)
EXPECTED_DATAPLEX_TASK_LINK = (
    DATAPLEX_BASE_LINK
    + f"process/tasks/{TEST_LAKE_ID}.{TEST_TASK_ID};location={TEST_LOCATION}/jobs?project={TEST_PROJECT_ID}"
)
EXPECTED_DATAPLEX_TASKS_LINK = (
    DATAPLEX_BASE_LINK + f"process/tasks?project={TEST_PROJECT_ID}&qLake={TEST_LAKE_ID}.{TEST_LOCATION}"
)
EXPECTED_DATAPLEX_CATALOG_ASPECT_TYPE_LINK = (
    DATAPLEX_BASE_LINK
    + f"projects/{TEST_PROJECT_ID}/locations/{TEST_LOCATION}/aspectTypes/{TEST_ASPECT_TYPE_ID}?project={TEST_PROJECT_ID}"
)
EXPECTED_DATAPLEX_CATALOG_ASPECT_TYPES_LINK = (
    DATAPLEX_BASE_LINK + f"catalog/aspect-types?project={TEST_PROJECT_ID}"
)
EXPECTED_DATAPLEX_CATALOG_ENTRY_LINK = (
    DATAPLEX_BASE_LINK
    + f"dp-entries/projects/{TEST_PROJECT_ID}/locations/{TEST_LOCATION}/entryGroups/{TEST_ENTRY_GROUP_ID}/entries/{TEST_ENTRY_ID}?project={TEST_PROJECT_ID}"
)


class TestDataplexTaskLink:
    @pytest.mark.db_test
    def test_get_link(self, dag_maker, create_task_instance_of_operator, session, mock_supervisor_comms):
        expected_url = EXPECTED_DATAPLEX_TASK_LINK
        link = DataplexTaskLink()
        ti = create_task_instance_of_operator(
            DataplexCreateTaskOperator,
            dag_id="test_link_dag",
            task_id="test_link_task",
            region=TEST_LOCATION,
            lake_id=TEST_LAKE_ID,
            project_id=TEST_PROJECT_ID,
            body=TEST_LAKE_BODY,
            dataplex_task_id=TEST_TASK_ID,
        )
        task = dag_maker.dag.get_task(ti.task_id)

        if AIRFLOW_V_3_0_PLUS and mock_supervisor_comms:
            mock_supervisor_comms.send.return_value = XComResult(
                key="key",
                value={
                    "lake_id": task.lake_id,
                    "task_id": task.dataplex_task_id,
                    "region": task.region,
                    "project_id": task.project_id,
                },
            )
        actual_url = link.get_link(operator=task, ti_key=ti.key)
        assert actual_url == expected_url


class TestDataplexTasksLink:
    @pytest.mark.db_test
    def test_get_link(self, dag_maker, create_task_instance_of_operator, session, mock_supervisor_comms):
        expected_url = EXPECTED_DATAPLEX_TASKS_LINK
        link = DataplexTasksLink()
        ti = create_task_instance_of_operator(
            DataplexListTasksOperator,
            dag_id="test_link_dag",
            task_id="test_link_task",
            region=TEST_LOCATION,
            lake_id=TEST_LAKE_ID,
            project_id=TEST_PROJECT_ID,
        )
        task = dag_maker.dag.get_task(ti.task_id)

        link.persist(context={"ti": ti, "task": task})

        if AIRFLOW_V_3_0_PLUS and mock_supervisor_comms:
            mock_supervisor_comms.send.return_value = XComResult(
                key="key",
                value={
                    "project_id": task.project_id,
                    "lake_id": task.lake_id,
                    "region": task.region,
                },
            )
        actual_url = link.get_link(operator=task, ti_key=ti.key)
        assert actual_url == expected_url


class TestDataplexLakeLink:
    @pytest.mark.db_test
    def test_get_link(self, dag_maker, create_task_instance_of_operator, session, mock_supervisor_comms):
        expected_url = DATAPLEX_LAKE_LINK
        link = DataplexLakeLink()
        ti = create_task_instance_of_operator(
            DataplexCreateLakeOperator,
            dag_id="test_link_dag",
            task_id="test_link_task",
            region=TEST_LOCATION,
            lake_id=TEST_LAKE_ID,
            project_id=TEST_PROJECT_ID,
            body={},
        )
        task = dag_maker.dag.get_task(ti.task_id)

        if AIRFLOW_V_3_0_PLUS and mock_supervisor_comms:
            mock_supervisor_comms.send.return_value = XComResult(
                key="key",
                value={
                    "lake_id": task.lake_id,
                    "region": task.region,
                    "project_id": task.project_id,
                },
            )
        actual_url = link.get_link(operator=task, ti_key=ti.key)
        assert actual_url == expected_url


class TestDataplexCatalogEntryGroupLink:
    @pytest.mark.db_test
    def test_get_link(self, dag_maker, create_task_instance_of_operator, session, mock_supervisor_comms):
        expected_url = EXPECTED_DATAPLEX_CATALOG_ENTRY_GROUP_LINK
        link = DataplexCatalogEntryGroupLink()
        ti = create_task_instance_of_operator(
            DataplexCatalogGetEntryGroupOperator,
            dag_id="test_link_dag",
            task_id="test_link_task",
            location=TEST_LOCATION,
            entry_group_id=TEST_ENTRY_GROUP_ID,
            project_id=TEST_PROJECT_ID,
        )
        task = dag_maker.dag.get_task(ti.task_id)

        link.persist(context={"ti": ti, "task": task})

        if AIRFLOW_V_3_0_PLUS and mock_supervisor_comms:
            mock_supervisor_comms.send.return_value = XComResult(
                key="key",
                value={
                    "entry_group_id": task.entry_group_id,
                    "location": task.location,
                    "project_id": task.project_id,
                },
            )
        actual_url = link.get_link(operator=task, ti_key=ti.key)
        assert actual_url == expected_url


class TestDataplexCatalogEntryGroupsLink:
    @pytest.mark.db_test
    def test_get_link(self, dag_maker, create_task_instance_of_operator, session, mock_supervisor_comms):
        expected_url = EXPECTED_DATAPLEX_CATALOG_ENTRY_GROUPS_LINK
        link = DataplexCatalogEntryGroupsLink()
        ti = create_task_instance_of_operator(
            DataplexCatalogCreateEntryGroupOperator,
            dag_id="test_link_dag",
            task_id="test_link_task",
            location=TEST_LOCATION,
            entry_group_id=TEST_ENTRY_GROUP_ID,
            entry_group_configuration=TEST_ENTRY_GROUP_ID_BODY,
            project_id=TEST_PROJECT_ID,
        )
        task = dag_maker.dag.get_task(ti.task_id)

        if AIRFLOW_V_3_0_PLUS and mock_supervisor_comms:
            mock_supervisor_comms.send.return_value = XComResult(
                key="key",
                value={
                    "location": task.location,
                    "project_id": task.project_id,
                },
            )
        actual_url = link.get_link(operator=task, ti_key=ti.key)
        assert actual_url == expected_url


class TestDataplexCatalogEntryTypeLink:
    @pytest.mark.db_test
    def test_get_link(self, dag_maker, create_task_instance_of_operator, session, mock_supervisor_comms):
        expected_url = EXPECTED_DATAPLEX_CATALOG_ENTRY_TYPE_LINK
        link = DataplexCatalogEntryTypeLink()
        ti = create_task_instance_of_operator(
            DataplexCatalogGetEntryTypeOperator,
            dag_id="test_link_dag",
            task_id="test_link_task",
            location=TEST_LOCATION,
            entry_type_id=TEST_ENTRY_TYPE_ID,
            project_id=TEST_PROJECT_ID,
        )
        task = dag_maker.dag.get_task(ti.task_id)

        if AIRFLOW_V_3_0_PLUS and mock_supervisor_comms:
            mock_supervisor_comms.send.return_value = XComResult(
                key="key",
                value={
                    "entry_type_id": task.entry_type_id,
                    "location": task.location,
                    "project_id": task.project_id,
                },
            )
        actual_url = link.get_link(operator=task, ti_key=ti.key)
        assert actual_url == expected_url


class TestDataplexCatalogEntryTypesLink:
    @pytest.mark.db_test
    def test_get_link(self, dag_maker, create_task_instance_of_operator, session, mock_supervisor_comms):
        expected_url = EXPECTED_DATAPLEX_CATALOG_ENTRY_TYPES_LINK
        link = DataplexCatalogEntryTypesLink()
        ti = create_task_instance_of_operator(
            DataplexCatalogCreateEntryTypeOperator,
            dag_id="test_link_dag",
            task_id="test_link_task",
            location=TEST_LOCATION,
            entry_type_id=TEST_ENTRY_TYPE_ID,
            entry_type_configuration=TEST_ENTRY_TYPE_ID_BODY,
            project_id=TEST_PROJECT_ID,
        )
        task = dag_maker.dag.get_task(ti.task_id)

        if AIRFLOW_V_3_0_PLUS and mock_supervisor_comms:
            mock_supervisor_comms.send.return_value = XComResult(
                key="key",
                value={
                    "location": task.location,
                    "project_id": task.project_id,
                },
            )
        actual_url = link.get_link(operator=task, ti_key=ti.key)
        assert actual_url == expected_url


class TestDataplexCatalogAspectTypeLink:
    @pytest.mark.db_test
    def test_get_link(self, dag_maker, create_task_instance_of_operator, session, mock_supervisor_comms):
        expected_url = EXPECTED_DATAPLEX_CATALOG_ASPECT_TYPE_LINK
        link = DataplexCatalogAspectTypeLink()
        ti = create_task_instance_of_operator(
            DataplexCatalogGetAspectTypeOperator,
            dag_id="test_link_dag",
            task_id="test_link_task",
            location=TEST_LOCATION,
            aspect_type_id=TEST_ASPECT_TYPE_ID,
            project_id=TEST_PROJECT_ID,
        )
        task = dag_maker.dag.get_task(ti.task_id)

        if AIRFLOW_V_3_0_PLUS and mock_supervisor_comms:
            mock_supervisor_comms.send.return_value = XComResult(
                key="key",
                value={
                    "aspect_type_id": task.aspect_type_id,
                    "location": task.location,
                    "project_id": task.project_id,
                },
            )
        actual_url = link.get_link(operator=task, ti_key=ti.key)
        assert actual_url == expected_url


class TestDataplexCatalogAspectTypesLink:
    @pytest.mark.db_test
    def test_get_link(self, dag_maker, create_task_instance_of_operator, session, mock_supervisor_comms):
        expected_url = EXPECTED_DATAPLEX_CATALOG_ASPECT_TYPES_LINK
        link = DataplexCatalogAspectTypesLink()
        ti = create_task_instance_of_operator(
            DataplexCatalogCreateAspectTypeOperator,
            dag_id="test_link_dag",
            task_id="test_link_task",
            location=TEST_LOCATION,
            aspect_type_id=TEST_ASPECT_TYPE_ID,
            aspect_type_configuration=TEST_ASPECT_TYPE_ID_BODY,
            project_id=TEST_PROJECT_ID,
        )
        task = dag_maker.dag.get_task(ti.task_id)

        if AIRFLOW_V_3_0_PLUS and mock_supervisor_comms:
            mock_supervisor_comms.send.return_value = XComResult(
                key="key",
                value={
                    "location": task.location,
                    "project_id": task.project_id,
                },
            )
        actual_url = link.get_link(operator=task, ti_key=ti.key)
        assert actual_url == expected_url


class TestDataplexCatalogEntryLink:
    @pytest.mark.db_test
    def test_get_link(self, dag_maker, create_task_instance_of_operator, session, mock_supervisor_comms):
        expected_url = EXPECTED_DATAPLEX_CATALOG_ENTRY_LINK
        link = DataplexCatalogEntryLink()
        ti = create_task_instance_of_operator(
            DataplexCatalogGetEntryOperator,
            dag_id="test_link_dag",
            task_id="test_link_task",
            location=TEST_LOCATION,
            entry_id=TEST_ENTRY_ID,
            entry_group_id=TEST_ENTRY_GROUP_ID,
            project_id=TEST_PROJECT_ID,
        )
        task = dag_maker.dag.get_task(ti.task_id)

        if AIRFLOW_V_3_0_PLUS and mock_supervisor_comms:
            mock_supervisor_comms.send.return_value = XComResult(
                key="key",
                value={
                    "entry_id": task.entry_id,
                    "entry_group_id": task.entry_group_id,
                    "location": task.location,
                    "project_id": task.project_id,
                },
            )
        actual_url = link.get_link(operator=task, ti_key=ti.key)
        assert actual_url == expected_url
