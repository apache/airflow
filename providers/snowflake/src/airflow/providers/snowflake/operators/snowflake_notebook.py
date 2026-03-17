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

from collections.abc import Sequence
from typing import Any

from airflow.providers.snowflake.operators.snowflake import SnowflakeSqlApiOperator


class SnowflakeNotebookOperator(SnowflakeSqlApiOperator):
    """
    Execute a Snowflake Notebook via the Snowflake SQL API.

    Builds an ``EXECUTE NOTEBOOK`` statement and delegates execution to
    :class:`~airflow.providers.snowflake.operators.snowflake.SnowflakeSqlApiOperator`,
    which handles query submission, polling, deferral, and cancellation.

    The operator supports the following authentication methods via the Snowflake connection:

    - **Key pair**: provide ``private_key_file`` or ``private_key_content`` in the connection extras.
    - **OAuth**: provide ``refresh_token``, ``client_id``, and ``client_secret`` in the connection extras.
    - **Programmatic Access Token (PAT)**: set ``authenticator`` to ``programmatic_access_token`` in
      the connection extras and put the PAT value in the connection ``password`` field.

    .. seealso::
        `Snowflake EXECUTE NOTEBOOK
        <https://docs.snowflake.com/en/sql-reference/sql/execute-notebook>`_

    :param notebook: Fully-qualified notebook name
        (e.g. ``MY_DB.MY_SCHEMA.MY_NOTEBOOK``).
    :param parameters: Optional list of parameter strings to pass to the
        notebook.  Only string values are supported by Snowflake; other
        data types are interpreted as NULL.  Parameters are accessible in
        the notebook via ``sys.argv``.
    :param snowflake_conn_id: Reference to the Snowflake connection.
    :param warehouse: Snowflake warehouse name (overrides connection default).
    :param database: Snowflake database name (overrides connection default).
    :param schema: Snowflake schema name (overrides connection default).
    :param role: Snowflake role name (overrides connection default).
    :param authenticator: Snowflake authenticator type.
    :param session_parameters: Snowflake session-level parameters.
    :param poll_interval: Seconds between status checks (default 5).
    :param token_life_time: Lifetime of the JWT token.
    :param token_renewal_delta: When to renew the JWT token before expiry.
    :param deferrable: If True, run in deferrable mode (frees the worker
        slot while waiting).  Defaults to the ``operators.default_deferrable``
        Airflow config.
    :param snowflake_api_retry_args: Optional dict of arguments forwarded to
        ``tenacity.Retrying`` for API call retries.
    """

    template_fields: Sequence[str] = tuple(
        set(SnowflakeSqlApiOperator.template_fields) | {"notebook", "parameters"}
    )

    def __init__(
        self,
        *,
        notebook: str,
        parameters: list[str] | None = None,
        **kwargs: Any,
    ) -> None:
        self.notebook = notebook
        self.parameters = parameters
        sql = self._build_execute_notebook_query()
        super().__init__(sql=sql, statement_count=1, **kwargs)

    def _build_execute_notebook_query(self) -> str:
        """Build the ``EXECUTE NOTEBOOK`` SQL statement."""
        params_clause = ""
        if self.parameters:
            escaped = ", ".join(f"'{p.replace(chr(39), chr(39) + chr(39))}'" for p in self.parameters)
            params_clause = escaped
        return f"EXECUTE NOTEBOOK {self.notebook}({params_clause})"
