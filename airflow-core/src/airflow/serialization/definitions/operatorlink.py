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

import json
from typing import TYPE_CHECKING

import attrs

from airflow.models.xcom import XComModel
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import create_session

if TYPE_CHECKING:
    from sqlalchemy import Row
    from sqlalchemy.orm import Session

    from airflow.models.taskinstancekey import TaskInstanceKey
    from airflow.serialization.definitions.mappedoperator import Operator

LINK_TRY_SUFFIX = "__try_"


def build_xcom_key_for_try(xcom_key: str, try_number: int) -> str:
    """Build the per try xcom key a link value is stored under."""
    return f"{xcom_key}{LINK_TRY_SUFFIX}{try_number}"


@attrs.define()
class XComOperatorLink(LoggingMixin):
    """
    Generic operator link class that can retrieve link only using XCOMs.

    Used while deserializing operators.
    """

    name: str
    xcom_key: str

    @staticmethod
    def _read_value(session: Session, key: str, ti_key: TaskInstanceKey) -> Row | None:
        return session.execute(
            XComModel.get_many(
                key=key,
                run_id=ti_key.run_id,
                dag_ids=ti_key.dag_id,
                task_ids=ti_key.task_id,
                map_indexes=ti_key.map_index,
            )
            .with_only_columns(XComModel.value)
            .limit(1)
        ).first()

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
            # Runs from before per-try keys existed only have the unsuffixed key.
            result = self._read_value(
                session, build_xcom_key_for_try(self.xcom_key, ti_key.try_number), ti_key
            ) or self._read_value(session, self.xcom_key, ti_key)
        if not result:
            self.log.debug(
                "No link with name: %s present in XCom as key: %s, returning empty link",
                self.name,
                self.xcom_key,
            )
            return ""

        from airflow.serialization.stringify import (
            StringifyNotSupportedError,
            stringify as stringify_xcom,
        )

        try:
            parsed_value = json.loads(result.value)
        except (ValueError, TypeError):
            # Handling for cases when types do not need to be deserialized (e.g. when value is a simple string link)
            parsed_value = result.value

        try:
            return str(stringify_xcom(parsed_value))
        except StringifyNotSupportedError:
            # If stringify doesn't support the type, return the raw value as a string.
            # This avoids the XComModel.deserialize_value() call that could
            # instantiate arbitrary classes from untrusted XCom data.
            return str(parsed_value)
