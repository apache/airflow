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

from abc import ABC, abstractmethod
from enum import Enum
from typing import Generic, NamedTuple, TypeVar

import re2 as re
from fastapi import HTTPException, Request, status
from sqlalchemy.exc import IntegrityError

T = TypeVar("T", bound=Exception)

_ColumnsType = dict[str, str]
# Key: column name, Value: column value


class BaseErrorHandler(Generic[T], ABC):
    """Base class for error handlers."""

    def __init__(self, exception_cls: T) -> None:
        self.exception_cls = exception_cls

    @abstractmethod
    def exception_handler(self, request: Request, exc: T):
        """exception_handler method."""
        raise NotImplementedError


class _DatabaseDialect(Enum):
    SQLITE = "sqlite"
    MYSQL = "mysql"
    POSTGRES = "postgres"


def _get_columns_dict_from_params_statement(params: tuple, statement: str) -> _ColumnsType:
    """Transform parameters and statement into a dictionary where the keys are column names, and the values are column values."""
    if not params or not statement:
        return {}
    column_names = statement.split("(")[1].split(")")[0].split(",")
    # replace ",',` and space with empty string
    column_names = [re.sub(r"[\"'` ]", "", column_name) for column_name in column_names]
    return {column_name: param for column_name, param in zip(column_names, params)}


class _UniqueConstraintErrorHandler(BaseErrorHandler[IntegrityError]):
    """Exception raised when trying to insert a duplicate value in a unique column."""

    class UniqueConstraintViolationDetail(NamedTuple):
        table_name: str | None
        error_columns: list[tuple[str, str]]
        parsed_error: bool

    response_error_format = (
        "Unique constraint violation: {table_name} with {error_column_pairs} already exists"
    )
    unique_constraint_error_prefix_dict: dict[_DatabaseDialect, str] = {
        _DatabaseDialect.SQLITE: "UNIQUE constraint failed",
        _DatabaseDialect.MYSQL: "Duplicate entry",
        _DatabaseDialect.POSTGRES: "violates unique constraint",
    }

    def __init__(self):
        super().__init__(IntegrityError)
        self.dialect: _DatabaseDialect.value | None = None

    def exception_handler(self, request: Request, exc: IntegrityError):
        """Handle IntegrityError exception."""
        if self._is_dialect_matched(exc):
            error_detail = self._parse_error_message(exc)
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=self.response_error_format.format(
                    table_name=error_detail.table_name,
                    error_column_pairs=", ".join(
                        [
                            f"{column_name}={column_value}"
                            for column_name, column_value in error_detail.error_columns
                        ]
                    ),
                )
                if not error_detail.parsed_error and error_detail.table_name is not None
                else f"Unique constraint violation: {str(exc.orig)}",
            )

    def _is_dialect_matched(self, exc: IntegrityError) -> bool:
        """Check if the exception matches the unique constraint error message for any dialect."""
        exc_orig_str = str(exc.orig)
        for dialect, error_msg in self.unique_constraint_error_prefix_dict.items():
            if error_msg in exc_orig_str:
                self.dialect = dialect
                return True
        return False

    def _parse_error_message(self, exc: IntegrityError) -> UniqueConstraintViolationDetail:
        """Parse the error message to extract the table name, column name and value."""
        str_exc_orig = str(exc.orig)
        table_name: None | str = None
        error_columns: list[tuple[str, str]] = []

        if self.dialect is _DatabaseDialect.SQLITE:
            # 'UNIQUE constraint failed: {table_name}.{column_name}'
            # Can be multiple columns in the unique constraint:
            # 'UNIQUE constraint failed: {table_name}.{column_name}, {table_name}.{column_name}, ...'
            matches = re.findall(r"([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)", str_exc_orig)
            if not matches:
                return self.UniqueConstraintViolationDetail(
                    table_name=None,
                    error_columns=error_columns,
                    parsed_error=True,
                )
            columns_dict = _get_columns_dict_from_params_statement(exc.params, str(exc.statement))
            for match in matches:
                if table_name is None:
                    table_name = match[0]
                column_name = match[1]
                error_columns.append((column_name, columns_dict[column_name]))

        if self.dialect is _DatabaseDialect.MYSQL:
            # "Duplicate entry '{column_value}' for key '{table_name}.{table_name}_{column_name}_uq'"
            # Can be multiple columns in the unique constraint:
            # "Duplicate entry '{column_value}-{column_value2}' for key '{table_name}.{table_name}_{column_name}_{column_name2}_uq'"
            pattern = re.compile(
                r"Duplicate entry '(?P<column_values>.*)' for key '(?P<table_name>.*)\.(?P<constraint_name>.*)'"
            )
            match = pattern.search(str_exc_orig)
            if not match:
                return self.UniqueConstraintViolationDetail(
                    table_name=None,
                    error_columns=error_columns,
                    parsed_error=True,
                )
            table_name = match.group("table_name")
            column_values = match.group("column_values").split("-")
            # since unique constraint suffix naming is not consistent, we can't extract column_name from the constraint name
            # try extracting column_name from the column_values
            columns_dict = _get_columns_dict_from_params_statement(exc.params, str(exc.statement))
            column_keys = [key for key, value in columns_dict.items() if value in column_values]
            # this could be wrong if column_value contains '-' or there are same values in multiple columns
            if len(column_keys) != len(column_values):
                return self.UniqueConstraintViolationDetail(
                    parsed_error=True,
                )
            error_columns = list(zip(column_keys, column_values))

        if self.dialect is _DatabaseDialect.POSTGRES:
            # 'duplicate key value violates unique constraint "{table_name}_{column_name}_uq"\nDETAIL:  Key (column_name)=(column_value) already exists.\n'
            # Can be multiple columns in the unique constraint:
            # "{table_name}_{column_name}_{column_name2}_uq"\nDETAIL:  Key (column_name, column_name2)=(column_value, column_value2) already exists.\n'
            # Use regex to extract KEY (column_name)=(column_value)
            column_pattern = re.compile(
                r"Key \((?P<column_keys>.*)\)=\((?P<column_values>.*)\) already exists"
            )
            matches = column_pattern.search(str_exc_orig)
            if not matches:
                return self.UniqueConstraintViolationDetail(
                    table_name=None,
                    error_columns=error_columns,
                    parsed_error=True,
                )
            column_keys = matches.group("column_keys").split(", ")
            column_values = matches.group("column_values").split(", ")
            error_columns = list(zip(column_keys, column_values))
            # extract table_name from the unique constraint name
            joined_column_keys = "_".join(sorted(column_keys))
            table_pattern = re.compile(rf"\"([a-zA-Z0-9_]+)_{re.escape(joined_column_keys)}.*\"")
            match = table_pattern.search(str_exc_orig)
            if match:
                table_name = match.group(1)

        if table_name is None or not error_columns:
            return self.UniqueConstraintViolationDetail(
                table_name=None,
                error_columns=error_columns,
                parsed_error=True,
            )

        return self.UniqueConstraintViolationDetail(
            table_name=table_name,
            error_columns=error_columns,
            parsed_error=False,
        )


DatabaseErrorHandlers = [
    _UniqueConstraintErrorHandler(),
]
