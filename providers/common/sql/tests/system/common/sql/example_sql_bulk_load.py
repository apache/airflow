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

from pathlib import Path

from airflow import DAG
from airflow.providers.common.sql.operators.sql import (
    SQLBulkLoadOperator,
)
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.timezone import datetime

TMP_FILE = "/tmp/actors.tsv"

connection_args = {
    "conn_id": "airflow_db",
    "conn_type": "Postgres",
    "host": "postgres",
    "schema": "postgres",
    "login": "postgres",
    "password": "postgres",
    "port": 5432,
}


def create_bulk_load_file():
    Path(TMP_FILE).write_text(
        "\n".join(
            [
                "Stallone\tSylvester\t78",
                "Statham\tJason\t57",
                "Li\tJet\t61",
                "Lundgren\tDolph\t66",
                "Norris\tChuck\t84",
            ]
        ),
        encoding="utf-8",
    )


with DAG(
    "example_sql_bulk_load",
    description="Example DAG for SQLBulkLoadOperator.",
    default_args=connection_args,
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    """
    ### Example SQL bulk load DAG

    Runs the SQLBulkLoadOperator against the Airflow metadata DB.
    """

    create_file = PythonOperator(
        task_id="create_file",
        python_callable=create_bulk_load_file,
    )

    # [START howto_operator_sql_bulk_load]
    bulk_load = SQLBulkLoadOperator(
        task_id="bulk_load",
        table="actors",
        tmp_file=TMP_FILE,
        preoperator=[
            """
            CREATE TABLE IF NOT EXISTS actors (
                name TEXT NOT NULL,
                firstname TEXT NOT NULL,
                age BIGINT NOT NULL
            );
            """,
            "TRUNCATE TABLE actors;",
        ],
        postoperator="DROP TABLE IF EXISTS actors;",
    )
    # [END howto_operator_sql_bulk_load]

    create_file >> bulk_load


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
