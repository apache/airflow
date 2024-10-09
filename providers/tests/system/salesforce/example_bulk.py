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

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.salesforce.operators.bulk import SalesforceBulkOperator

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_salesforce_bulk"

with DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    # [START howto_salesforce_bulk_insert_operation]
    bulk_insert = SalesforceBulkOperator(
        task_id="bulk_insert",
        operation="insert",
        object_name="Account",
        payload=[
            {"Id": "000000000000000AAA", "Name": "account1"},
            {"Name": "account2"},
        ],
        external_id_field="Id",
        batch_size=10000,
        use_serial=False,
    )
    # [END howto_salesforce_bulk_insert_operation]

    # [START howto_salesforce_bulk_update_operation]
    bulk_update = SalesforceBulkOperator(
        task_id="bulk_update",
        operation="update",
        object_name="Account",
        payload=[
            {"Id": "000000000000000AAA", "Name": "account1"},
            {"Id": "000000000000000BBB", "Name": "account2"},
        ],
        batch_size=10000,
        use_serial=False,
    )
    # [END howto_salesforce_bulk_update_operation]

    # [START howto_salesforce_bulk_upsert_operation]
    bulk_upsert = SalesforceBulkOperator(
        task_id="bulk_upsert",
        operation="upsert",
        object_name="Account",
        payload=[
            {"Id": "000000000000000AAA", "Name": "account1"},
            {"Name": "account2"},
        ],
        external_id_field="Id",
        batch_size=10000,
        use_serial=False,
    )
    # [END howto_salesforce_bulk_upsert_operation]

    # [START howto_salesforce_bulk_delete_operation]
    bulk_delete = SalesforceBulkOperator(
        task_id="bulk_delete",
        operation="delete",
        object_name="Account",
        payload=[
            {"Id": "000000000000000AAA"},
            {"Id": "000000000000000BBB"},
        ],
        batch_size=10000,
        use_serial=False,
    )
    # [END howto_salesforce_bulk_delete_operation]


from dev.tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
