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

from sqlalchemy import func

from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.parameters import QuerySkipRecordCount, QueryTablesFilter
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.db import MetadataDBStatsResponse, TableStats
from airflow.utils.db import reflect_tables
from airflow.utils.db_cleanup import _effective_table_names

db_router = AirflowRouter(tags=["Maintenance"], prefix="/maintenance/db")


@db_router.get("/stats", dependencies=[])
def get_db_stats(
    tables: QueryTablesFilter,
    session: SessionDep,
    skip_record_count: QuerySkipRecordCount = False,
) -> MetadataDBStatsResponse:
    existing_tables = reflect_tables(tables=None, session=session).tables
    _, effective_config_dict = _effective_table_names(table_names=tables)
    stats = []
    for table_name, table_config in effective_config_dict.items():
        if table_name not in existing_tables:
            continue
        orm_model = table_config.orm_model
        rec_col = table_config.recency_column

        oldest_ts = session.query(func.min(rec_col)).select_from(orm_model).scalar()

        total_rows = (
            session.query(func.count()).select_from(orm_model).scalar() if not skip_record_count else None
        )

        stats.append(
            TableStats(
                table_name=table_name,
                oldest_record=oldest_ts,
                record_count=total_rows,
            )
        )
    return MetadataDBStatsResponse(tables=stats)
