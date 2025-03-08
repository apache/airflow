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

from pgvector.psycopg2 import register_vector

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


class PgVectorIngestOperator(SQLExecuteQueryOperator):
    """
    This operator is designed for ingesting data into a PostgreSQL database with pgvector support.

    It inherits from the SQLExecuteQueryOperator and extends its functionality by registering
    the pgvector data type with the database connection before executing queries.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:PgVectorIngestOperator`

    """

    def __init__(self, *args, **kwargs) -> None:
        """Initialize a new PgVectorIngestOperator."""
        super().__init__(*args, **kwargs)

    def _register_vector(self) -> None:
        """Register the vector type with your connection."""
        conn = self.get_db_hook().get_conn()
        register_vector(conn)

    def execute(self, context):
        self._register_vector()
        super().execute(context)
