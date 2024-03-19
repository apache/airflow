from __future__ import annotations

from typing import TYPE_CHECKING, Iterable, Mapping
from contextlib import closing

from airflow.models import BaseOperator
from airflow.hooks.base import BaseHook
from airflow.providers.common.sql.hooks.sql import DbApiHook

import re

if TYPE_CHECKING:
    from airflow.utils.context import Context

_PROVIDERS_MATCHER = re.compile(r"airflow\.providers\.(.*)\.hooks.*")

_MIN_SUPPORTED_PROVIDERS_VERSION = {
    "amazon": "4.1.0",
    "apache.drill": "2.1.0",
    "apache.druid": "3.1.0",
    "apache.hive": "3.1.0",
    "apache.pinot": "3.1.0",
    "databricks": "3.1.0",
    "elasticsearch": "4.1.0",
    "exasol": "3.1.0",
    "google": "8.2.0",
    "jdbc": "3.1.0",
    "mssql": "3.1.0",
    "mysql": "3.1.0",
    "odbc": "3.1.0",
    "oracle": "3.1.0",
    "postgres": "5.1.0",
    "presto": "3.1.0",
    "qubole": "3.1.0",
    "slack": "5.1.0",
    "snowflake": "3.1.0",
    "sqlite": "3.1.0",
    "trino": "3.1.0",
    "vertica": "3.1.0",
}

class SqlToSqlOperator(BaseOperator):

    template_fields: Sequence[str] = ("source_sql", "source_sql_parameters")
    template_fields_renderers = {"source_sql": "sql", "source_sql_parameters": "json"}

    ui_color = "#e08c8c"

    def __init__(
            self,
            *,
            source_conn_id: str,
            destination_conn_id: str,
            destination_table: str,
            source_sql: str,
            source_sql_parameters: Mapping | Iterable | None = None,
            destination_hook_params: dict | None = None,
            source_hook_params: dict | None = None,
            rows_chunk: int = 5000,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        
        self.source_conn_id = source_conn_id
        self.destination_conn_id = destination_conn_id
        self.destination_table = destination_table
        self.source_sql = source_sql
        self.source_sql_parameters = source_sql_parameters
        self.destination_hook_params = destination_hook_params
        self.source_hook_params = source_hook_params
        self.rows_chunk = rows_chunk
        self.dest_db = "common"
        self.dest_schema = None

    def _hook(
            self,
            conn_id: str,
            hook_params:  Mapping | Iterable | None = None,
            dest: bool = False
    ):
        self.log.debug("Get connection for %s", conn_id)
        conn = BaseHook.get_connection(conn_id)
        hook = conn.get_hook(hook_params=hook_params)
        
        conn_dest_specific = [ "oracle", "snowflake" ]
        conn_dict = conn.to_dict()

        if dest:
            if "conn_type" in conn_dict.keys() and conn_dict["conn_type"] in conn_dest_specific:
                self.dest_db = conn_dict["conn_type"]

        if not isinstance(hook, DbApiHook):
            from airflow.hooks.dbapi_hook import DbApiHook as _DbApiHook
            
            if not isinstance(hook, _DbApiHook):
                class_module = hook.__class__.__module__
                match = _PROVIDERS_MATCHER.match(class_module)
                if match:
                    provider = match.group(1)
                    min_version = _MIN_SUPPORTED_PROVIDERS_VERSION.get(provider)
                    if min_version:
                        raise AirflowException(
                                f"You are trying to use common-sql with {hook.__class__.__name__},"
                                f" but the Hook class comes from provider {provider} that does not support it."
                                f" Please upgrade provider {provider} to at least {min_version}."
                        )
           
            raise AirflowException(
                    f"You are trying to use `common-sql` with {hook.__class__.__name__},"
                    " but its provider does not support it. Please upgrade the provider to a version that"
                    " supports `common-sql`. The hook class should be a subclass of"
                    " `airflow.providers.common.sql.hooks.sql.DbApiHook`."
                    f" Got {hook.__class__.__name__} Hook with class hierarchy: {hook.__class__.mro()}"
            )

        return hook

    def get_db_hook(
            self,
            conn_id: str,
            hook_params: Mapping | Iterable | None = None,
            dest: bool = False
    ) -> DbApiHook:
        return self._hook(conn_id, hook_params, dest)
    
    def _transfer_data(
            self,
            src_hook,
            dest_hook,
            context: Context
    ) -> None:
        self.log.info("Using Common insert mode")
        self.log.info("Querying data from source: %s", self.source_conn_id)
        self.log.info("Executing: %s", self.source_sql)
        
        with src_hook.get_cursor() as src_cursor:
            if self.source_sql_parameters:
                src_cursor.execute(self.source_sql, self.source_sql_parameters)
            else:
                src_cursor.execute(self.source_sql)

            target_fields = [field[0] for field in src_cursor.description]
            
            rows_total = 0

            for rows in iter(lambda: src_cursor.fetchmany(self.rows_chunk), []):
                dest_hook.insert_rows(
                    table = self.destination_table,
                    rows = rows,
                    target_fields=target_fields,
                    commit_every = self.rows_chunk
                )
                rows_total += len(rows)
        
            self.log.info("Total inserted: %s rows", rows_total)         
        
        self.log.info("Finished data transfer.")       

    def _oracle_tranfer_data(
            self,
            src_hook,
            dest_hook,
            context: Context
    ) -> None:
        self.log.info("Using Oracle bulk insert mode")
        self.log.info("Querying data from source: %s", self.source_conn_id)
        self.log.info("Executing: %s", self.source_sql)

        with src_hook.get_cursor() as src_cursor:
            if self.source_sql_parameters:
                src_cursor.execute(self.source_sql, self.source_sql_parameters)
            else:
                src_cursor.execute(self.source_sql)

            target_fields = [field[0] for field in src_cursor.description]

            rows_total = 0
            
            from airflow.providers.oracle.hooks.oracle import OracleHook
            
            for rows in iter(lambda: src_cursor.fetchmany(self.rows_chunk), []):
                dest_hook.bulk_insert_rows(
                    table = self.destination_table,
                    rows = rows,
                    target_fields=target_fields,
                    commit_every=self.rows_chunk
                )
                rows_total += len(rows)
            
            self.log.info("Total inserted: %s rows", rows_total)

        self.log.info("Finished data transfer.")
    
    def _snowflake_transfer_data(
            self,
            src_hook,
            dest_hook,
            context: Context
    ) -> None:
        self.log.info("Using Snowflake bulk insert mode")
        self.log.info("Querying data from source: %s", self.source_conn_id)
        self.log.info("Executing: %s", self.source_sql)

        with closing(src_hook.get_conn()) as src_conn:
            with closing(dest_hook.get_conn()) as dest_conn:
                rows_total = 0

                from pandas.io import sql as psql
                from snowflake.connector.pandas_tools import write_pandas

                for rows_df in psql.read_sql(self.source_sql, con=src_conn, params=self.source_sql_parameters, chunksize=self.rows_chunk):
                    write_pandas(
                        conn=dest_conn,
                        df=rows_df,
                        table_name=self.destination_table,
                        schema=dest_conn.schema,
                        chunk_size=self.rows_chunk,
                        auto_create_table=False,
                        overwrite=False
                    )
                    
                    rows_total += len(rows_df)

                self.log.info("Total inserted: %s rows", rows_total)

        self.log.info("Finished data transfer.")


    def execute(
            self, 
            context: Context
    ) -> None:
        src_hook = self.get_db_hook(self.source_conn_id, self.source_hook_params)
        dest_hook = self.get_db_hook(self.destination_conn_id, self.destination_hook_params, True)
        if self.dest_db == "oracle":
            self._oracle_tranfer_data(src_hook, dest_hook, context)
        if self.dest_db == "snowflake":
            self._snowflake_transfer_data(src_hook, dest_hook, context)
        else:
            elf._transfer_data(src_hook, dest_hook, context)
