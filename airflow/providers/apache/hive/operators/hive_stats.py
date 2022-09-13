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

import json
import warnings
from collections import OrderedDict
from typing import TYPE_CHECKING, Any, Callable, List, Sequence

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.apache.hive.hooks.hive import HiveMetastoreHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.presto.hooks.presto import PrestoHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class HiveStatsCollectionOperator(BaseOperator):
    """
    Gathers partition statistics using a dynamically generated Presto
    query, inserts the stats into a MySql table with this format. Stats
    overwrite themselves if you rerun the same date/partition. ::

        CREATE TABLE hive_stats (
            ds VARCHAR(16),
            table_name VARCHAR(500),
            metric VARCHAR(200),
            value BIGINT
        );

    :param metastore_conn_id: Reference to the
        :ref:`Hive Metastore connection id <howto/connection:hive_metastore>`.
    :param table: the source table, in the format ``database.table_name``. (templated)
    :param partition: the source partition. (templated)
    :param extra_exprs: dict of expression to run against the table where
        keys are metric names and values are Presto compatible expressions
    :param excluded_columns: list of columns to exclude, consider
        excluding blobs, large json columns, ...
    :param assignment_func: a function that receives a column name and
        a type, and returns a dict of metric names and an Presto expressions.
        If None is returned, the global defaults are applied. If an
        empty dictionary is returned, no stats are computed for that
        column.
    """

    template_fields: Sequence[str] = ('table', 'partition', 'ds', 'dttm')
    ui_color = '#aff7a6'

    def __init__(
        self,
        *,
        table: str,
        partition: Any,
        extra_exprs: dict[str, Any] | None = None,
        excluded_columns: list[str] | None = None,
        assignment_func: Callable[[str, str], dict[Any, Any] | None] | None = None,
        metastore_conn_id: str = 'metastore_default',
        presto_conn_id: str = 'presto_default',
        mysql_conn_id: str = 'airflow_db',
        **kwargs: Any,
    ) -> None:
        if 'col_blacklist' in kwargs:
            warnings.warn(
                f"col_blacklist kwarg passed to {self.__class__.__name__} "
                f"(task_id: {kwargs.get('task_id')}) is deprecated, "
                f"please rename it to excluded_columns instead",
                category=FutureWarning,
                stacklevel=2,
            )
            excluded_columns = kwargs.pop('col_blacklist')
        super().__init__(**kwargs)
        self.table = table
        self.partition = partition
        self.extra_exprs = extra_exprs or {}
        self.excluded_columns = excluded_columns or []  # type: List[str]
        self.metastore_conn_id = metastore_conn_id
        self.presto_conn_id = presto_conn_id
        self.mysql_conn_id = mysql_conn_id
        self.assignment_func = assignment_func
        self.ds = '{{ ds }}'
        self.dttm = '{{ execution_date.isoformat() }}'

    def get_default_exprs(self, col: str, col_type: str) -> dict[Any, Any]:
        """Get default expressions"""
        if col in self.excluded_columns:
            return {}
        exp = {(col, 'non_null'): f"COUNT({col})"}
        if col_type in {'double', 'int', 'bigint', 'float'}:
            exp[(col, 'sum')] = f'SUM({col})'
            exp[(col, 'min')] = f'MIN({col})'
            exp[(col, 'max')] = f'MAX({col})'
            exp[(col, 'avg')] = f'AVG({col})'
        elif col_type == 'boolean':
            exp[(col, 'true')] = f'SUM(CASE WHEN {col} THEN 1 ELSE 0 END)'
            exp[(col, 'false')] = f'SUM(CASE WHEN NOT {col} THEN 1 ELSE 0 END)'
        elif col_type == 'string':
            exp[(col, 'len')] = f'SUM(CAST(LENGTH({col}) AS BIGINT))'
            exp[(col, 'approx_distinct')] = f'APPROX_DISTINCT({col})'

        return exp

    def execute(self, context: Context) -> None:
        metastore = HiveMetastoreHook(metastore_conn_id=self.metastore_conn_id)
        table = metastore.get_table(table_name=self.table)
        field_types = {col.name: col.type for col in table.sd.cols}

        exprs: Any = {('', 'count'): 'COUNT(*)'}
        for col, col_type in list(field_types.items()):
            if self.assignment_func:
                assign_exprs = self.assignment_func(col, col_type)
                if assign_exprs is None:
                    assign_exprs = self.get_default_exprs(col, col_type)
            else:
                assign_exprs = self.get_default_exprs(col, col_type)
            exprs.update(assign_exprs)
        exprs.update(self.extra_exprs)
        exprs = OrderedDict(exprs)
        exprs_str = ",\n        ".join(f"{v} AS {k[0]}__{k[1]}" for k, v in exprs.items())

        where_clause_ = [f"{k} = '{v}'" for k, v in self.partition.items()]
        where_clause = " AND\n        ".join(where_clause_)
        sql = f"SELECT {exprs_str} FROM {self.table} WHERE {where_clause};"

        presto = PrestoHook(presto_conn_id=self.presto_conn_id)
        self.log.info('Executing SQL check: %s', sql)
        row = presto.get_first(sql)
        self.log.info("Record: %s", row)
        if not row:
            raise AirflowException("The query returned None")

        part_json = json.dumps(self.partition, sort_keys=True)

        self.log.info("Deleting rows from previous runs if they exist")
        mysql = MySqlHook(self.mysql_conn_id)
        sql = f"""
        SELECT 1 FROM hive_stats
        WHERE
            table_name='{self.table}' AND
            partition_repr='{part_json}' AND
            dttm='{self.dttm}'
        LIMIT 1;
        """
        if mysql.get_records(sql):
            sql = f"""
            DELETE FROM hive_stats
            WHERE
                table_name='{self.table}' AND
                partition_repr='{part_json}' AND
                dttm='{self.dttm}';
            """
            mysql.run(sql)

        self.log.info("Pivoting and loading cells into the Airflow db")
        rows = [
            (self.ds, self.dttm, self.table, part_json) + (r[0][0], r[0][1], r[1]) for r in zip(exprs, row)
        ]
        mysql.insert_rows(
            table='hive_stats',
            rows=rows,
            target_fields=[
                'ds',
                'dttm',
                'table_name',
                'partition_repr',
                'col',
                'metric',
                'value',
            ],
        )
