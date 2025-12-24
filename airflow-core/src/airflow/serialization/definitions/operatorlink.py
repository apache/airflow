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

from typing import TYPE_CHECKING, TypeAlias

import attrs

from airflow.models.xcom import XComModel
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import create_session

if TYPE_CHECKING:
    from airflow.models.mappedoperator import MappedOperator as SerializedMappedOperator
    from airflow.models.taskinstancekey import TaskInstanceKey
    from airflow.serialization.definitions.baseoperator import SerializedBaseOperator

    Operator: TypeAlias = "SerializedMappedOperator | SerializedBaseOperator"


@attrs.define()
class XComOperatorLink(LoggingMixin):
    """
    Generic operator link class that can retrieve link only using XCOMs.

    Used while deserializing operators.
    """

    name: str
    xcom_key: str

    def get_link(self, operator: Operator, *, ti_key: TaskInstanceKey) -> str:
        """
        Retrieve the link from the XComs.

        :param operator: The Airflow operator object this link is associated to.
        :param ti_key: TaskInstance ID to return link for.
        :return: link to external system, but by pulling it from XComs
        """
        self.log.info(
            "Attempting to retrieve link from XComs with key: %s for task id: %s", self.xcom_key, ti_key
        )
        with create_session() as session:
            value = session.execute(
                XComModel.get_many(
                    key=self.xcom_key,
                    run_id=ti_key.run_id,
                    dag_ids=ti_key.dag_id,
                    task_ids=ti_key.task_id,
                    map_indexes=ti_key.map_index,
                ).with_only_columns(XComModel.value)
            ).first()
        if not value:
            self.log.debug(
                "No link with name: %s present in XCom as key: %s, returning empty link",
                self.name,
                self.xcom_key,
            )
            return ""
        return XComModel.deserialize_value(value)
