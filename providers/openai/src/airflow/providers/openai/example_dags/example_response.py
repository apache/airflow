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
#
# [START quickstart_response]
from __future__ import annotations

# This is for Airflow 2.11. For Airflow 3+, use `from airflow.sdk import dag`
from airflow.providers.common.compat.sdk import dag
from airflow.providers.openai.operators.openai import OpenAIResponseOperator


@dag(tags=["example"])
def quickstart_openai():
    OpenAIResponseOperator(
        task_id="generate_response",
        conn_id="openai_default",
        input_text="Write a one-sentence summary of Apache Airflow.",
    )


quickstart_openai()
# [END quickstart_response]
