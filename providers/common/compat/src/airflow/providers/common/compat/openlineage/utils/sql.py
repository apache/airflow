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

import logging
from typing import TYPE_CHECKING

log = logging.getLogger(__name__)

if TYPE_CHECKING:
    from airflow.providers.openlineage.sqlparser import get_openlineage_facets_with_sql

else:
    try:
        from airflow.providers.openlineage.sqlparser import get_openlineage_facets_with_sql
    except ImportError:

        def get_openlineage_facets_with_sql(
            hook,
            sql: str | list[str],
            conn_id: str,
            database: str | None,
        ):
            try:
                from airflow.providers.openlineage.sqlparser import SQLParser
            except ImportError:
                log.debug("SQLParser could not be imported from OpenLineage provider.")
                return None

            try:
                from airflow.providers.openlineage.utils.utils import should_use_external_connection

                use_external_connection = should_use_external_connection(hook)
            except ImportError:
                # OpenLineage provider release < 1.8.0 - we always use connection
                use_external_connection = True

            connection = hook.get_connection(conn_id)
            try:
                database_info = hook.get_openlineage_database_info(connection)
            except AttributeError:
                log.debug("%s has no database info provided", hook)
                database_info = None

            if database_info is None:
                return None

            try:
                sql_parser = SQLParser(
                    dialect=hook.get_openlineage_database_dialect(connection),
                    default_schema=hook.get_openlineage_default_schema(),
                )
            except AttributeError:
                log.debug("%s failed to get database dialect", hook)
                return None

            try:
                sqlalchemy_engine = hook.get_sqlalchemy_engine()
            except Exception as e:
                log.debug("Failed to get sql alchemy engine: %s", e)
                sqlalchemy_engine = None

            operator_lineage = sql_parser.generate_openlineage_metadata_from_sql(
                sql=sql,
                hook=hook,
                database_info=database_info,
                database=database,
                sqlalchemy_engine=sqlalchemy_engine,
                use_connection=use_external_connection,
            )

            return operator_lineage


__all__ = ["get_openlineage_facets_with_sql"]
