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

from unittest.mock import MagicMock

import pytest

from airflow.providers.amazon.aws.links.emr import EmrClusterLink
from airflow.serialization.serialized_objects import SerializedDAG
from tests.providers.amazon.aws.utils.links_test_utils import link_test_operator

DAG_ID = "test_dag_id"
TASK_ID = "test_task_id"
JOB_FLOW_ID = "j-test-flow-id"
REGION_NAME = "eu-west-1"


@pytest.fixture(scope="module")
def operator_class():
    return link_test_operator(EmrClusterLink)


@pytest.fixture(scope="module")
def mock_task(operator_class):
    return operator_class(task_id=TASK_ID)


@pytest.fixture()
def mock_ti(create_task_instance_of_operator, operator_class):
    return create_task_instance_of_operator(operator_class, dag_id=DAG_ID, task_id=TASK_ID)


@pytest.mark.need_serialized_dag
class TestEmrClusterLink:
    def test_link_serialize(self, dag_maker, mock_ti):
        serialized_dag = dag_maker.get_serialized_data()

        assert serialized_dag["dag"]["tasks"][0]["_operator_extra_links"] == [
            {"airflow.providers.amazon.aws.links.emr.EmrClusterLink": {}}
        ], "Operator links should exist for serialized DAG"

    @pytest.mark.parametrize(
        "region_name,aws_partition,aws_domain",
        [
            ("eu-west-1", "aws", "aws.amazon.com"),
            ("cn-north-1", "aws-cn", "amazonaws.cn"),
            ("us-gov-east-1", "aws-us-gov", "amazonaws-us-gov.com"),
        ],
    )
    def test_emr_custer_link(self, dag_maker, mock_task, mock_ti, region_name, aws_partition, aws_domain):
        mock_context = MagicMock()
        mock_context.__getitem__.side_effect = {"ti": mock_ti}.__getitem__

        EmrClusterLink.persist(
            context=mock_context,
            operator=mock_task,
            region_name=region_name,
            aws_partition=aws_partition,
            job_flow_id=JOB_FLOW_ID,
        )

        expected = (
            f"https://console.{aws_domain}/elasticmapreduce/home?region={region_name}"
            f"#cluster-details:{JOB_FLOW_ID}"
        )
        assert (
            mock_ti.task.get_extra_links(mock_ti, EmrClusterLink.name) == expected
        ), "Operator link should be preserved after execution"

        serialized_dag = dag_maker.get_serialized_data()
        deserialized_dag = SerializedDAG.from_dict(serialized_dag)
        deserialized_task = deserialized_dag.task_dict[TASK_ID]

        assert (
            deserialized_task.get_extra_links(mock_ti, EmrClusterLink.name) == expected
        ), "Operator link should be preserved in deserialized tasks after execution"

    def test_empty_xcom(self, dag_maker, mock_ti):
        serialized_dag = dag_maker.get_serialized_data()
        deserialized_dag = SerializedDAG.from_dict(serialized_dag)
        deserialized_task = deserialized_dag.task_dict[TASK_ID]

        assert (
            mock_ti.task.get_extra_links(mock_ti, EmrClusterLink.name) == ""
        ), "Operator link should only be added if job id is available in XCom"

        assert (
            deserialized_task.get_extra_links(mock_ti, EmrClusterLink.name) == ""
        ), "Operator link should be empty for deserialized task with no XCom push"
