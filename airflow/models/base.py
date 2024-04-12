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

from collections import deque
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Mapping

from sqlalchemy import Column, Constraint, Index, Integer, MetaData, String, text
from sqlalchemy.orm import Mapped, Mapper, declared_attr, registry

from airflow.configuration import conf

if TYPE_CHECKING:
    from sqlalchemy.schema import Table


_SA_TABLE_ARGS = "_table_args_"
_SA_MAPPER_ARGS = "_mapper_args_"
SQL_ALCHEMY_SCHEMA = conf.get("database", "SQL_ALCHEMY_SCHEMA")

# For more information about what the tokens in the naming convention
# below mean, see:
# https://docs.sqlalchemy.org/en/14/core/metadata.html#sqlalchemy.schema.MetaData.params.naming_convention
naming_convention = {
    "ix": "idx_%(column_0_N_label)s",
    "uq": "%(table_name)s_%(column_0_N_name)s_uq",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "%(table_name)s_%(column_0_name)s_fkey",
    "pk": "%(table_name)s_pkey",
}


def _get_schema():
    if not SQL_ALCHEMY_SCHEMA or SQL_ALCHEMY_SCHEMA.isspace():
        return None
    return SQL_ALCHEMY_SCHEMA


metadata = MetaData(schema=_get_schema(), naming_convention=naming_convention)
mapper_registry = registry(metadata=metadata)
_sentinel = object()


ID_LEN = 250


@mapper_registry.as_declarative_base()
class Base:
    """sqlalchemy mapper base class."""

    _table_args_: ClassVar[Callable[[], tuple[Any, ...]] | tuple[Any, ...] | None]
    _mapper_args_: ClassVar[Callable[[], Mapping[str, Any]] | Mapping[str, Any] | None]

    __abstract__: ClassVar[bool] = True
    __table__: ClassVar[Table]
    __mapper__: ClassVar[Mapper]
    metadata: ClassVar[MetaData]

    if TYPE_CHECKING:
        __table_args__: ClassVar[tuple[Any, ...]]
        __mapper_args__: ClassVar[dict[str, Any]]

        def __init__(self, *args: Any, **kwargs: Any) -> None: ...
    else:

        @declared_attr
        def __table_args__(cls: type[Any]) -> tuple[Any, ...]:
            return _resolve_table_args(cls)

        @declared_attr
        def __mapper_args__(cls: type[Any]) -> dict[str, Any]:
            return _resolve_mapper_args(cls)


def get_id_collation_args():
    """Get SQLAlchemy args to use for COLLATION."""
    collation = conf.get("database", "sql_engine_collation_for_ids", fallback=None)
    if collation:
        return {"collation": collation}
    else:
        # Automatically use utf8mb3_bin collation for mysql
        # This is backwards-compatible. All our IDS are ASCII anyway so even if
        # we migrate from previously installed database with different collation and we end up mixture of
        # COLLATIONS, it's not a problem whatsoever (and we keep it small enough so that our indexes
        # for MYSQL will not exceed the maximum index size.
        #
        # See https://github.com/apache/airflow/pull/17603#issuecomment-901121618.
        #
        # We cannot use session/dialect as at this point we are trying to determine the right connection
        # parameters, so we use the connection
        conn = conf.get("database", "sql_alchemy_conn", fallback="")
        if conn.startswith(("mysql", "mariadb")):
            return {"collation": "utf8mb3_bin"}
        return {}


COLLATION_ARGS: dict[str, Any] = get_id_collation_args()


def StringID(*, length=ID_LEN, **kwargs) -> String:
    return String(length=length, **kwargs, **COLLATION_ARGS)


class TaskInstanceDependencies(Base):
    """Base class for depending models linked to TaskInstance."""

    __abstract__ = True
    _table_args_ = lambda: (
        Column("task_id", StringID(), nullable=False),
        Column("dag_id", StringID(), nullable=False),
        Column("run_id", StringID(), nullable=False),
        Column("map_index", Integer(), nullable=False, server_default=text("-1")),
    )

    task_id: Mapped[str]
    dag_id: Mapped[str]
    run_id: Mapped[str]
    map_index: Mapped[int]


def _resolve_table_args(table_class: type[Base]) -> tuple[Any, ...]:
    if not issubclass(table_class, Base):
        raise TypeError("not sqlalchemy table class")

    table_args: Callable[[], tuple[Any, ...]] | tuple[Any, ...] | None
    table_args_queue: deque[Any] = deque()
    table_args_mapping: dict[str, Any] = {}

    for table_upper_class in table_class.mro():
        if table_upper_class is Base:
            break

        table_args = getattr(table_upper_class, _SA_TABLE_ARGS, None)
        if callable(table_args):
            table_args = table_args()
        if not table_args:
            continue

        last_sa_args = table_args[-1]
        if isinstance(last_sa_args, Mapping):
            table_args_mapping.update(last_sa_args)
            table_args = table_args[:-1]

        table_args_queue.extend(table_args)

    registries: tuple[set[str], set[str], set[str]] = (set(), set(), set())
    table_args = tuple(
        table_arg
        for element in table_args_queue
        if (table_arg := _resolve_table_arg_elements(element, registries)) is not None
    )
    if table_args_mapping:
        return *table_args, table_args_mapping
    return table_args


def _resolve_table_arg_elements(element: Any, registries: tuple[set[str], set[str], set[str]]) -> Any:
    if isinstance(element, Column):
        registry = registries[0]
    elif isinstance(element, Index):
        registry = registries[1]
    elif isinstance(element, Constraint):
        registry = registries[2]
    else:
        raise NotImplementedError

    if not element.name:
        raise NotImplementedError

    if element.name in registry:
        return None
    registry.add(element.name)
    return element


def _resolve_mapper_args(mapper_class: type[Base]) -> dict[str, Any]:
    if not issubclass(mapper_class, Base):
        raise TypeError("not sqlalchemy mapper class")

    mapper_args: Callable[[], Mapping[str, Any]] | Mapping[str, Any] | None
    mapper_args_mapping: dict[str, Any] = {}

    for mapper_upper_class in mapper_class.mro():
        if mapper_upper_class is Base:
            break

        mapper_args = getattr(mapper_upper_class, _SA_MAPPER_ARGS, None)
        if callable(mapper_args):
            mapper_args = mapper_args()
        if not mapper_args:
            continue

        mapper_args_mapping.update(mapper_args)

    return mapper_args_mapping

def _validate_mapper_properties(mapper_class: type[Base]) -> None:
    mapper_class.__annotations__.keys()