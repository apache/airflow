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

from typing import ClassVar

from airflow.providers.amazon.aws.hooks.duckdb import AwsDuckDBHook
from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

try:
    from airflow.providers.duckdb.operators.duckdb import DuckDBExecuteQueryOperator
except ImportError:
    raise AirflowOptionalProviderFeatureException(
        "This feature requires the 'duckdb' provider to be installed. Install it with: "
        "pip install 'apache-airflow-providers-amazon[duckdb]'"
    )


class AwsDuckDBOperator(DuckDBExecuteQueryOperator):
    """
    Run SQL against an in-process DuckDB database with AWS credentials already wired up.

    Behaves like
    :class:`~airflow.providers.duckdb.operators.duckdb.DuckDBExecuteQueryOperator` — including not
    requiring an Airflow DuckDB connection to exist — but opens the database through
    :class:`~airflow.providers.amazon.aws.hooks.duckdb.AwsDuckDBHook`, so the ``httpfs`` and ``aws``
    extensions are loaded and an S3 secret is created from an Airflow AWS connection.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AwsDuckDBOperator`

    :param sql: the SQL statement, list of statements, or ``.sql`` template file to run.
    :param conn_id: DuckDB connection to use. Need not exist, in which case an in-memory database
        is used.
    :param database: DuckDB database to open. Overrides the connection.
    :param hook_params: extra keyword arguments for
        :class:`~airflow.providers.amazon.aws.hooks.duckdb.AwsDuckDBHook`, for example
        ``{"aws_conn_id": "aws_prod", "region_name": "us-west-2", "extensions": ["iceberg"]}``.
    """

    # Swapping the hook class is the whole difference from the base operator, which builds the hook
    # from this attribute.
    hook_class: ClassVar[type[AwsDuckDBHook]] = AwsDuckDBHook
