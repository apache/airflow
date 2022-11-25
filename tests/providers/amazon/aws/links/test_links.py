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

from operator import itemgetter
from unittest.mock import MagicMock

import pytest

from airflow.providers.amazon.aws.links.batch import (
    BatchJobDefinitionLink,
    BatchJobDetailsLink,
    BatchJobQueueLink,
)
from airflow.providers.amazon.aws.links.emr import EmrClusterLink
from airflow.providers.amazon.aws.links.logs import CloudWatchEventsLink
from airflow.serialization.serialized_objects import SerializedDAG

TASK_ID = "test_task_id"
REGION_NAME = "eu-west-1"
AWS_PARTITION = "aws"
AWS_DOMAIN = "aws.amazon.com"


def _full_qualname(cls) -> str:
    return f"{cls.__module__}.{cls.__qualname__}"


# List AWS External Link with test cases
# Expected list op tuple(BaseAwsLink, kwargs, expected URL)
AWS_LINKS = [
    (
        BatchJobDefinitionLink,
        {"job_definition_arn": "arn:aws:batch:dummy-region:111111111111:job-definition/batch-test:42"},
        f"https://console.{AWS_DOMAIN}/batch/home?region={REGION_NAME}"
        f"#job-definition/detail/arn:aws:batch:dummy-region:111111111111:job-definition/batch-test:42",
    ),
    (
        BatchJobDetailsLink,
        {"job_id": "00000000-0000-0000-0000-000000000000"},
        f"https://console.{AWS_DOMAIN}/batch/home?region={REGION_NAME}"
        f"#jobs/detail/00000000-0000-0000-0000-000000000000",
    ),
    (
        BatchJobQueueLink,
        {"job_queue_arn": "arn:aws:batch:dummy-region:111111111111:job-queue/test-queue"},
        f"https://console.{AWS_DOMAIN}/batch/home?region={REGION_NAME}"
        f"#queues/detail/arn:aws:batch:dummy-region:111111111111:job-queue/test-queue",
    ),
    (
        EmrClusterLink,
        {"job_flow_id": "j-TEST-FLOW-ID"},
        f"https://console.{AWS_DOMAIN}/elasticmapreduce/home?region={REGION_NAME}"
        f"#cluster-details:j-TEST-FLOW-ID",
    ),
    (
        CloudWatchEventsLink,
        {
            "awslogs_region": "ap-southeast-2",
            "awslogs_group": "/test/logs/group",
            "awslogs_stream_name": "test/stream/d56a66bb98a14c4593defa1548686edf",
        },
        f"https://console.{AWS_DOMAIN}/cloudwatch/home?region=ap-southeast-2"
        f"#logsV2:log-groups/log-group/%2Ftest%2Flogs%2Fgroup"
        f"/log-events/test%2Fstream%2Fd56a66bb98a14c4593defa1548686edf",
    ),
]


@pytest.mark.need_serialized_dag
class TestAwsLinks:
    @pytest.mark.parametrize("extra_link_class", map(itemgetter(0), AWS_LINKS), ids=_full_qualname)
    def test_link_serialize(self, extra_link_class, dag_maker, create_task_and_ti_of_op_with_extra_link):
        """Test: Operator links should exist for serialized DAG."""
        create_task_and_ti_of_op_with_extra_link(
            extra_link_class, dag_id="test_link_serialize", task_id=TASK_ID
        )
        serialized_dag = dag_maker.get_serialized_data()
        assert serialized_dag["dag"]["tasks"][0]["_operator_extra_links"] == [
            {f"{_full_qualname(extra_link_class)}": {}}
        ], "Operator links should exist for serialized DAG"

    @pytest.mark.parametrize("extra_link_class", map(itemgetter(0), AWS_LINKS), ids=_full_qualname)
    def test_empty_xcom(self, extra_link_class, dag_maker, create_task_and_ti_of_op_with_extra_link):
        """Test: Operator links should return empty string if no XCom value."""
        _, ti = create_task_and_ti_of_op_with_extra_link(
            extra_link_class, dag_id="test_empty_xcom", task_id=TASK_ID
        )

        serialized_dag = dag_maker.get_serialized_data()
        deserialized_dag = SerializedDAG.from_dict(serialized_dag)
        deserialized_task = deserialized_dag.task_dict[TASK_ID]

        assert (
            ti.task.get_extra_links(ti, extra_link_class.name) == ""
        ), "Operator link should only be added if job id is available in XCom"

        assert (
            deserialized_task.get_extra_links(ti, extra_link_class.name) == ""
        ), "Operator link should be empty for deserialized task with no XCom push"

    @pytest.mark.parametrize("extra_link_class, extra_link_kwargs, extra_link_expected_url", AWS_LINKS)
    def test_extra_link(
        self,
        extra_link_class,
        extra_link_kwargs,
        extra_link_expected_url,
        dag_maker,
        create_task_and_ti_of_op_with_extra_link,
    ):
        """Test: Expected URL Link."""
        task, ti = create_task_and_ti_of_op_with_extra_link(
            extra_link_class, dag_id="test_extra_link", task_id=TASK_ID
        )

        mock_context = MagicMock()
        mock_context.__getitem__.side_effect = {"ti": ti}.__getitem__

        extra_link_class.persist(
            context=mock_context,
            operator=task,
            region_name=REGION_NAME,
            aws_partition=AWS_PARTITION,
            **extra_link_kwargs,
        )

        assert (
            ti.task.get_extra_links(ti, extra_link_class.name) == extra_link_expected_url
        ), f"{_full_qualname(extra_link_class)} should be preserved after execution"

        serialized_dag = dag_maker.get_serialized_data()
        deserialized_dag = SerializedDAG.from_dict(serialized_dag)
        deserialized_task = deserialized_dag.task_dict[TASK_ID]

        assert (
            deserialized_task.get_extra_links(ti, extra_link_class.name) == extra_link_expected_url
        ), f"{_full_qualname(extra_link_class)} should be preserved in deserialized tasks after execution"
