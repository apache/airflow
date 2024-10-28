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
"""
Example use of Snowflake Snowpark Python related decorators.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from snowflake.snowpark import Session

from airflow import DAG
from airflow.decorators import task

SNOWFLAKE_CONN_ID = "my_snowflake_conn"
DAG_ID = "example_snowpark_decorator"

with DAG(
    DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule="@once",
    default_args={"snowflake_conn_id": SNOWFLAKE_CONN_ID},
    tags=["example"],
    catchup=False,
) as dag:
    # [START howto_decorator_snowpark]
    @task.snowpark
    def setup_data(session: Session):
        # The Snowpark session object is injected as an argument
        data = [
            (1, 0, 5, "Product 1", "prod-1", 1, 10),
            (2, 1, 5, "Product 1A", "prod-1-A", 1, 20),
            (3, 1, 5, "Product 1B", "prod-1-B", 1, 30),
            (4, 0, 10, "Product 2", "prod-2", 2, 40),
            (5, 4, 10, "Product 2A", "prod-2-A", 2, 50),
            (6, 4, 10, "Product 2B", "prod-2-B", 2, 60),
            (7, 0, 20, "Product 3", "prod-3", 3, 70),
            (8, 7, 20, "Product 3A", "prod-3-A", 3, 80),
            (9, 7, 20, "Product 3B", "prod-3-B", 3, 90),
            (10, 0, 50, "Product 4", "prod-4", 4, 100),
            (11, 10, 50, "Product 4A", "prod-4-A", 4, 100),
            (12, 10, 50, "Product 4B", "prod-4-B", 4, 100),
        ]
        columns = [
            "id",
            "parent_id",
            "category_id",
            "name",
            "serial_number",
            "key",
            "3rd",
        ]
        df = session.create_dataframe(data, schema=columns)
        table_name = "sample_product_data"
        df.write.save_as_table(table_name, mode="overwrite")
        return table_name

    table_name = setup_data()  # type: ignore[call-arg]

    @task.snowpark
    def check_num_rows(table_name: str):
        # Alternatively, retrieve the Snowpark session object using `get_active_session`
        from snowflake.snowpark.context import get_active_session

        session = get_active_session()
        df = session.table(table_name)
        assert df.count() == 12

    check_num_rows(table_name)
    # [END howto_decorator_snowpark]


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
