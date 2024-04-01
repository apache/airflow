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

from datetime import datetime

from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.bedrock import BedrockInvokeModelOperator
from tests.system.providers.amazon.aws.utils import SystemTestContextBuilder

sys_test_context_task = SystemTestContextBuilder().build()

DAG_ID = "example_bedrock"
PROMPT = "What color is an orange?"

with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    tags=["example"],
    catchup=False,
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context["ENV_ID"]

    # [START howto_operator_invoke_llama_model]
    invoke_llama_model = BedrockInvokeModelOperator(
        task_id="invoke_llama",
        model_id="meta.llama2-13b-chat-v1",
        input_data={"prompt": PROMPT},
    )
    # [END howto_operator_invoke_llama_model]

    # [START howto_operator_invoke_titan_model]
    invoke_titan_model = BedrockInvokeModelOperator(
        task_id="invoke_titan",
        model_id="amazon.titan-text-express-v1",
        input_data={"inputText": PROMPT},
    )
    # [END howto_operator_invoke_titan_model]

    chain(
        # TEST SETUP
        test_context,
        # TEST BODY
        invoke_llama_model,
        invoke_titan_model,
        # TEST TEARDOWN
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
