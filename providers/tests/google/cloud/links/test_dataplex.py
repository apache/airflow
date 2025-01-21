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
    DataplexCatalogEntryGroupLink,
    DataplexCatalogEntryGroupsLink,
    DataplexCatalogEntryTypeLink,
    DataplexCatalogEntryTypesLink,
    DataplexLakeLink,
    DataplexTaskLink,
    DataplexTasksLink,
)
from airflow.providers.google.cloud.operators.dataplex import (
    DataplexCatalogCreateEntryGroupOperator,
    DataplexCatalogCreateEntryTypeOperator,
    DataplexCatalogGetEntryGroupOperator,
    DataplexCatalogGetEntryTypeOperator,
    DataplexCreateLakeOperator,
    DataplexCreateTaskOperator,
    DataplexListTasksOperator,
)

TEST_LOCATION = "test-location"
TEST_PROJECT_ID = "test-project-id"
TEST_ENTRY_GROUP_ID = "test-entry-group-id"
TEST_ENTRY_GROUP_ID_BODY = {"description": "some description"}
TEST_ENTRY_GROUPS_ID = "test-entry-groups-id"
TEST_ENTRY_TYPE_ID = "test-entry-type-id"
TEST_ENTRY_TYPE_ID_BODY = {"description": "some description"}
TEST_ENTRY_TYPES_ID = "test-entry-groups-id"
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


class TestDataplexTaskLink:
    @pytest.mark.db_test
    def test_get_link(self, create_task_instance_of_operator, session):
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
        session.add(ti)
        session.commit()
        link.persist(context={"ti": ti}, task_instance=ti.task)
        actual_url = link.get_link(operator=ti.task, ti_key=ti.key)
        assert actual_url == expected_url


class TestDataplexTasksLink:
    @pytest.mark.db_test
    def test_get_link(self, create_task_instance_of_operator, session):
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
        session.add(ti)
        session.commit()
        link.persist(context={"ti": ti}, task_instance=ti.task)
        actual_url = link.get_link(operator=ti.task, ti_key=ti.key)
        assert actual_url == expected_url


class TestDataplexLakeLink:
    @pytest.mark.db_test
    def test_get_link(self, create_task_instance_of_operator, session):
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
        session.add(ti)
        session.commit()
        link.persist(context={"ti": ti}, task_instance=ti.task)
        actual_url = link.get_link(operator=ti.task, ti_key=ti.key)
        assert actual_url == expected_url


class TestDataplexCatalogEntryGroupLink:
    @pytest.mark.db_test
    def test_get_link(self, create_task_instance_of_operator, session):
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
        session.add(ti)
        session.commit()
        link.persist(context={"ti": ti}, task_instance=ti.task)
        actual_url = link.get_link(operator=ti.task, ti_key=ti.key)
        assert actual_url == expected_url


class TestDataplexCatalogEntryGroupsLink:
    @pytest.mark.db_test
    def test_get_link(self, create_task_instance_of_operator, session):
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
        session.add(ti)
        session.commit()
        link.persist(context={"ti": ti}, task_instance=ti.task)
        actual_url = link.get_link(operator=ti.task, ti_key=ti.key)
        assert actual_url == expected_url


class TestDataplexCatalogEntryTypeLink:
    @pytest.mark.db_test
    def test_get_link(self, create_task_instance_of_operator, session):
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
        session.add(ti)
        session.commit()
        link.persist(context={"ti": ti}, task_instance=ti.task)
        actual_url = link.get_link(operator=ti.task, ti_key=ti.key)
        assert actual_url == expected_url


class TestDataplexCatalogEntryTypesLink:
    @pytest.mark.db_test
    def test_get_link(self, create_task_instance_of_operator, session):
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
        session.add(ti)
        session.commit()
        link.persist(context={"ti": ti}, task_instance=ti.task)
        actual_url = link.get_link(operator=ti.task, ti_key=ti.key)
        assert actual_url == expected_url
