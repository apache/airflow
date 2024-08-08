from functools import lru_cache
from typing import TYPE_CHECKING

from sqlalchemy.engine import Inspector

from airflow.utils.log.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from airflow.providers.common.sql.hooks.sql import DbApiHook


class Dialect(LoggingMixin):
    def __init__(self, hook: DbApiHook):
        super().__init__()
        self.hook = hook

    @property
    def placeholder(self) -> str:
        return self.hook.placeholder

    @property
    def inspector(self) -> Inspector:
        return self.hook.inspector

    @property
    def _insert_statement_format(self) -> str:
        return self.hook._insert_statement_format

    @property
    def _replace_statement_format(self) -> str:
        return self.hook._replace_statement_format

    @classmethod
    def _extract_schema_from_table(cls, table: str) -> list[str]:
        return table.split(".")[::-1]

    @lru_cache
    def get_column_names(self, table: str) -> list[str]:
        column_names = list(
            column["name"]
            for column in self.inspector.get_columns(
                *self._extract_schema_from_table(table)
            )
        )
        self.log.debug("Column names for table '%s': %s", table, column_names)
        return column_names

    @lru_cache
    def get_primary_keys(self, table: str) -> list[str]:
        primary_keys = self.inspector.get_pk_constraint(
            *self._extract_schema_from_table(table)
        ).get("constrained_columns", [])
        self.log.debug("Primary keys for table '%s': %s", table, primary_keys)
        return primary_keys

    def generate_insert_sql(self, table, values, target_fields, **kwargs) -> str:
        """
        Generate the INSERT SQL statement.

        :param table: Name of the target table
        :param values: The row to insert into the table
        :param target_fields: The names of the columns to fill in the table
        :return: The generated INSERT SQL statement
        """

        placeholders = [
            self.placeholder,
        ] * len(values)

        if target_fields:
            target_fields = ", ".join(target_fields)
            target_fields = f"({target_fields})"
        else:
            target_fields = ""

        return self._insert_statement_format.format(
            table, target_fields, ",".join(placeholders)
        )

    def generate_replace_sql(self, table, values, target_fields, **kwargs) -> str:
        """
        Generate the REPLACE SQL statement.

        :param table: Name of the target table
        :param values: The row to insert into the table
        :param target_fields: The names of the columns to fill in the table
        :return: The generated REPLACE SQL statement
        """

        placeholders = [
            self.placeholder,
        ] * len(values)

        if target_fields:
            target_fields = ", ".join(target_fields)
            target_fields = f"({target_fields})"
        else:
            target_fields = ""

        return self._replace_statement_format.format(
            table, target_fields, ",".join(placeholders)
        )
