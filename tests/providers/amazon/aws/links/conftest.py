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

from airflow.providers.amazon.aws.links.base_aws import BaseAwsLink
from tests.providers.amazon.aws.utils.links_test_utils import link_test_operator


@pytest.fixture()
def create_task_and_ti_of_op_with_extra_link(create_task_instance_of_operator):
    def _create_op_and_ti(
        extra_link_class: BaseAwsLink,
        *,
        dag_id,
        task_id,
        execution_date=None,
        session=None,
        **operator_kwargs,
    ):
        op = link_test_operator(extra_link_class)
        return op(task_id=task_id), create_task_instance_of_operator(
            link_test_operator(extra_link_class),
            dag_id=dag_id,
            task_id=task_id,
            execution_date=execution_date,
            session=session,
            **operator_kwargs,
        )

    return _create_op_and_ti
