from functools import lru_cache

from airflow.providers.common.sql.dialects.dialect import Dialect
from airflow.providers.common.sql.hooks.sql import fetch_all_handler


class MsSqlDialect(Dialect):
    @lru_cache
    def get_primary_keys(self, table: str) -> list[str]:
        primary_keys = self.hook.run(
            f"""
                SELECT c.name
                FROM sys.columns c
                WHERE c.object_id =  OBJECT_ID('{table}')
                    AND EXISTS (SELECT 1 FROM sys.index_columns ic
                        INNER JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                        WHERE i.is_primary_key = 1
                        AND ic.object_id = c.object_id
                        AND ic.column_id = c.column_id);
                """,
            handler=fetch_all_handler,
        )
        primary_keys = [pk[0] for pk in primary_keys]
        self.log.debug("Primary keys for table '%s': %s", table, primary_keys)
        return primary_keys

    def generate_replace_sql(self, table, values, target_fields, **kwargs) -> str:
        primary_keys = self.get_primary_keys(table)
        columns = [
            target_field
            for target_field in target_fields
            if target_field in set(target_fields).difference(set(primary_keys))
        ]

        self.log.debug("primary_keys: %s", primary_keys)
        self.log.debug("columns: %s", columns)

        return f"""MERGE INTO {table} AS target
            USING (SELECT {', '.join(map(lambda column: f'{self.placeholder} AS {column}', target_fields))}) AS source
            ON {' AND '.join(map(lambda column: f'target.{column} = source.{column}', primary_keys))}
            WHEN MATCHED THEN
                UPDATE SET {', '.join(map(lambda column: f'target.{column} = source.{column}', columns))}
            WHEN NOT MATCHED THEN
                INSERT ({', '.join(target_fields)}) VALUES ({', '.join(map(lambda column: f'source.{column}', target_fields))});"""
