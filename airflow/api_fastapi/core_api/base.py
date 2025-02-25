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
from typing import TYPE_CHECKING, Generic, TypeVar

from pydantic import BaseModel as PydanticBaseModel, ConfigDict

if TYPE_CHECKING:
    from sqlalchemy.sql import Select

T = TypeVar("T")


class BaseModel(PydanticBaseModel):
    """
    Base pydantic model for REST API.

    :meta private:
    """

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


class StrictBaseModel(BaseModel):
    """
    StrictBaseModel is a base Pydantic model for REST API that does not allow any extra fields.

    Use this class for models that should not have any extra fields in the payload.

    :meta private:
    """

    model_config = ConfigDict(from_attributes=True, populate_by_name=True, extra="forbid")


class OrmClause(Generic[T], ABC):
    """
    Base class for filtering clauses with paginated_select.

    The subclasses should implement the `to_orm` method and set the `value` attribute.
    """

    def __init__(self, value: T | None = None):
        self.value = value

    @abstractmethod
    def to_orm(self, select: Select) -> Select:
        pass
