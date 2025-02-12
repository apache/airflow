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

from abc import ABCMeta, abstractmethod
from typing import TYPE_CHECKING, ClassVar

import attrs

from airflow.models.xcom import BaseXCom
from airflow.utils.log.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from airflow.models.baseoperator import BaseOperator
    from airflow.models.taskinstancekey import TaskInstanceKey


@attrs.define()
class XComOperatorLink(LoggingMixin):
    """A generic operator link class that can retrieve link only using XCOMs. Used while deserializing operators."""

    name: str
    xcom_key: str

    def get_link(self, operator: BaseOperator, *, ti_key: TaskInstanceKey) -> str:
        """
        Retrieve the link from the XComs.

        :param operator: The Airflow operator object this link is associated to.
        :param ti_key: TaskInstance ID to return link for.
        :return: link to external system, but by pulling it from XComs
        """
        self.log.info(
            "Attempting to retrieve link from XComs with key: %s for task id: %s", self.xcom_key, ti_key
        )
        value = BaseXCom.get_one(
            key=self.xcom_key,
            run_id=ti_key.run_id,
            dag_id=ti_key.dag_id,
            task_id=ti_key.task_id,
            map_index=ti_key.map_index,
        )
        if not value:
            self.log.debug(
                "No link with name: %s present in XCom as key: %s, returning empty link",
                self.name,
                self.xcom_key,
            )
            return ""
        # Stripping is a temporary workaround till https://github.com/apache/airflow/issues/46513 is handled.
        return value.strip('"')


@attrs.define()
class BaseOperatorLink(metaclass=ABCMeta):
    """Abstract base class that defines how we get an operator link."""

    operators: ClassVar[list[type[BaseOperator]]] = []
    """
    This property will be used by Airflow Plugins to find the Operators to which you want
    to assign this Operator Link

    :return: List of Operator classes used by task for which you want to create extra link
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Name of the link. This will be the button name on the task UI."""

    @property
    def xcom_key(self) -> str:
        """
        XCom key with while the whole "link" for this operator link is stored.

        On retrieving with this key, the entire link is returned.

        Defaults to `_link_<class name>` if not provided.
        """
        return f"_link_{self.__class__.__name__}"

    @abstractmethod
    def get_link(self, operator: BaseOperator, *, ti_key: TaskInstanceKey) -> str:
        """
        Link to external system.

        Note: The old signature of this function was ``(self, operator, dttm: datetime)``. That is still
        supported at runtime but is deprecated.

        :param operator: The Airflow operator object this link is associated to.
        :param ti_key: TaskInstance ID to return link for.
        :return: link to external system
        """
