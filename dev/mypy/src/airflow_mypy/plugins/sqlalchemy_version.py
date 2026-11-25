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

from importlib.metadata import PackageNotFoundError, version
from typing import TYPE_CHECKING

from mypy.plugin import Plugin

if TYPE_CHECKING:
    from mypy.options import Options

SQLALCHEMY_2_1_CONSTANT = "SQLALCHEMY_2_1"


def is_sqlalchemy_2_1_or_later(sqlalchemy_version: str) -> bool:
    major, minor = (int(part) for part in sqlalchemy_version.split(".")[:2])
    return (major, minor) >= (2, 1)


class SQLAlchemyVersionPlugin(Plugin):
    """Expose the installed SQLAlchemy version to mypy as the ``SQLALCHEMY_2_1`` constant.

    SQLAlchemy 2.1 parameterises ``Select`` with a ``TypeVarTuple`` (``Select[X, Y]``) where 2.0
    takes the row tuple (``Select[tuple[X, Y]]``). Mypy cannot branch on a library version, so
    this registers ``SQLALCHEMY_2_1`` as always true or always false, letting a type alias pick
    the right spelling for whichever SQLAlchemy mypy is reading.
    """

    def __init__(self, options: Options) -> None:
        try:
            sqlalchemy_version = version("sqlalchemy")
        except PackageNotFoundError:
            pass
        else:
            if is_sqlalchemy_2_1_or_later(sqlalchemy_version):
                options.always_true.append(SQLALCHEMY_2_1_CONSTANT)
            else:
                options.always_false.append(SQLALCHEMY_2_1_CONSTANT)
        super().__init__(options)


def plugin(version: str):
    return SQLAlchemyVersionPlugin
