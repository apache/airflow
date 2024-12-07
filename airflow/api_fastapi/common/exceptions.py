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
    POSTGRESQL = "postgresql"


def _get_columns_dict_from_params_statement(params: tuple, statement: str) -> _ColumnsType:
    """Transform parameters and statement into a dictionary where the keys are column names, and the values are column values."""
    column_names = statement.split("(")[1].split(")")[0].split(",")
    column_names = [column_name.strip() for column_name in column_names]
    return {column_name: param for column_name, param in zip(column_names, params)}


class _UniqueConstraintErrorHandler(BaseErrorHandler[IntegrityError]):
    """Exception raised when trying to insert a duplicate value in a unique column."""

    class UniqueConstraintViolationDetail(NamedTuple):
        table_name: str | None = None
        column_name: str | None = None
        column_value: str | None = None
        parsed_error: bool = False

    def __init__(self):
        super().__init__(IntegrityError)
        self.response_error_format = (
            "Unique constraint violation: {table_name} with {column_name}={column_value} already exists"
        )
        self.unique_constraint_error_messages: dict[str, str] = {
            _DatabaseDialect.SQLITE: "UNIQUE constraint failed",
            _DatabaseDialect.MYSQL: "Duplicate entry",
            _DatabaseDialect.POSTGRESQL: "violates unique constraint",
        }
        self.dialect: _DatabaseDialect.value | None = None

    def exception_handler(self, request: Request, exc: IntegrityError):
        """Handle IntegrityError exception."""
        if self._is_dialect_matched(exc):
            error_detail = self._parse_error_message(exc)
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=self.response_error_format.format(
                    table_name=error_detail.table_name,
                    column_name=error_detail.column_name,
                    column_value=error_detail.column_value,
                )
                if not error_detail.parsed_error
                else f"Unique constraint violation: {str(exc.orig)}",
            )

    def _is_dialect_matched(self, exc: IntegrityError) -> bool:
        """Check if the exception matches the unique constraint error message for any dialect."""
        exc_orig_str = str(exc.orig)
        for dialect, error_msg in self.unique_constraint_error_messages.items():
            if error_msg in exc_orig_str:
                self.dialect = dialect
                return True
        return False

    def _parse_error_message(self, exc: IntegrityError) -> UniqueConstraintViolationDetail:
        """Parse the error message to extract the table name, column name and value."""
        str_exc_orig = str(exc.orig)
        table_name: None | str = None
        error_column_name: None | str = None
        column_value: None | str = None

        if self.dialect is _DatabaseDialect.SQLITE:
            # 'UNIQUE constraint failed: {table_name}.{column_name}'
            table_name, error_column_name = str_exc_orig.split(":")[1].split(".")
            table_name = table_name.strip()
            error_column_name = error_column_name.strip()
            column_value = _get_columns_dict_from_params_statement(exc.params, exc.statement)[
                error_column_name
            ]

        if self.dialect is _DatabaseDialect.MYSQL:
            # "Duplicate entry '{column_value}' for key '{table_name}.{table_name}_{column_name}_uq'"
            pattern = re.compile(
                r"Duplicate entry '(?P<column_value>.*)' for key '(?P<table_name>.*)\.(?P<constraint_name>.*)'"
            )
            match = pattern.search(str_exc_orig)
            constraint_name = None
            if match:
                table_name = match.group("table_name")
                column_value = match.group("column_value")
                constraint_name = match.group("constraint_name")
            # extract column_name from the constraint name
            column_pattern = re.compile(rf"{table_name}_(?P<column_name>.*)_uq")
            match = column_pattern.search(constraint_name)
            if match:
                error_column_name = match.group("column_name")

        if self.dialect is _DatabaseDialect.POSTGRESQL:
            # 'duplicate key value violates unique constraint "{table_name}_{column_name}_uq"\nDETAIL:  Key (column_name)=(column_value) already exists.\n'
            # use regex to extract KEY (column_name)=(column_value)
            column_pattern = re.compile(
                r"Key \((?P<column_name>.*)\)=\((?P<column_value>.*)\) already exists"
            )
            match = column_pattern.search(str_exc_orig)
            if match:
                error_column_name = match.group("column_name")
                column_value = match.group("column_value")
            # extract table_name from the constraint name
            table_pattern = re.compile(rf"\"([a-zA-Z0-9_]+)_{re.escape(error_column_name)}_uq\"")
            match = table_pattern.search(str_exc_orig)
            if match:
                table_name = match.group(1)

        if table_name is None or error_column_name is None or column_value is None:
            return self.UniqueConstraintViolationDetail(
                parsed_error=True,
            )

        return self.UniqueConstraintViolationDetail(
            table_name=table_name,
            column_name=error_column_name,
            column_value=column_value,
        )


DatabaseErrorHandlers = [
    _UniqueConstraintErrorHandler(),
]
