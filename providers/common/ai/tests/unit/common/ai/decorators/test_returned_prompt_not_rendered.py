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

from unittest.mock import patch

import pytest

from airflow.providers.common.ai.decorators.agent import _AgentDecoratedOperator
from airflow.providers.common.ai.decorators.llm import _LLMDecoratedOperator
from airflow.providers.common.ai.decorators.llm_branch import _LLMBranchDecoratedOperator
from airflow.providers.common.ai.decorators.llm_file_analysis import _LLMFileAnalysisDecoratedOperator
from airflow.providers.common.ai.decorators.llm_schema_compare import _LLMSchemaCompareDecoratedOperator
from airflow.providers.common.ai.decorators.llm_sql import _LLMSQLDecoratedOperator
from airflow.providers.common.ai.operators.agent import AgentOperator
from airflow.providers.common.ai.operators.llm import LLMOperator
from airflow.providers.common.ai.operators.llm_branch import LLMBranchOperator
from airflow.providers.common.ai.operators.llm_file_analysis import LLMFileAnalysisOperator
from airflow.providers.common.ai.operators.llm_schema_compare import LLMSchemaCompareOperator
from airflow.providers.common.ai.operators.llm_sql import LLMSQLQueryOperator

_COMMON = {"llm_conn_id": "my_llm"}


@pytest.mark.parametrize(
    ("decorated_cls", "operator_cls", "extra_kwargs"),
    [
        pytest.param(_LLMDecoratedOperator, LLMOperator, {}, id="llm"),
        pytest.param(_AgentDecoratedOperator, AgentOperator, {}, id="agent"),
        pytest.param(_LLMSQLDecoratedOperator, LLMSQLQueryOperator, {}, id="llm_sql"),
        pytest.param(_LLMBranchDecoratedOperator, LLMBranchOperator, {}, id="llm_branch"),
        pytest.param(
            _LLMSchemaCompareDecoratedOperator,
            LLMSchemaCompareOperator,
            {"db_conn_ids": ["postgres_default", "snowflake_default"], "table_names": ["t"]},
            id="llm_schema_compare",
        ),
        pytest.param(
            _LLMFileAnalysisDecoratedOperator,
            LLMFileAnalysisOperator,
            {"file_path": "/tmp/app.log"},
            id="llm_file_analysis",
        ),
    ],
)
@pytest.mark.parametrize(
    "returned_prompt",
    ["Invoice total shows {{ 100 * 3 }} instead of 300", "Template snippet: {% raw %} unbalanced"],
    ids=["expression", "unbalanced-raw"],
)
def test_returned_prompt_reaches_operator_unchanged(
    decorated_cls, operator_cls, extra_kwargs, returned_prompt
):
    """Text returned by the callable is not rendered as a template before the operator runs."""
    op = decorated_cls(
        task_id="test",
        python_callable=lambda: returned_prompt,
        **_COMMON,
        **extra_kwargs,
    )
    with patch.object(operator_cls, "execute", autospec=True) as mock_execute:
        op.execute(context={})

    mock_execute.assert_called_once()
    assert mock_execute.call_args.args[0].prompt == returned_prompt
