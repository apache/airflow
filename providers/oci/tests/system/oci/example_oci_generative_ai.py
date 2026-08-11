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

try:
    from airflow.sdk import DAG, task
except ImportError:
    from airflow import DAG  # type: ignore[attr-defined,no-redef]
    from airflow.decorators import task  # type: ignore[attr-defined,no-redef]

DAG_ID = "example_oci_generative_ai"

with DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["example", "oci", "generative-ai"],
) as dag:

    @task
    def list_generative_ai_resources() -> None:
        """Validate read-only access to OCI Generative AI resources."""
        from airflow.providers.oci.hooks.generative_ai import OciGenerativeAIHook

        hook = OciGenerativeAIHook()
        compartment_id = hook.get_compartment_id()
        hook.conn.list_hosted_applications(compartment_id=compartment_id)
        hook.conn.list_hosted_applications_iam(compartment_id=compartment_id)
        hook.conn.list_hosted_deployments(compartment_id=compartment_id)

    list_generative_ai_resources()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

test_run = get_test_run(dag)
