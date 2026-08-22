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

from airflow._shared.state import TaskScope, attempt_link_state_key
from airflow.models.xcom import XComModel
from airflow.state import get_state_backend
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import create_session

if TYPE_CHECKING:
    from airflow.models.taskinstancekey import TaskInstanceKey
    from airflow.serialization.definitions.mappedoperator import Operator


@attrs.define()
class XComOperatorLink(LoggingMixin):
    """
    Generic operator link class that can retrieve link only using XCOMs.

    Used while deserializing operators.
    """

    name: str
    xcom_key: str

    def _stored_link(self, ti_key: TaskInstanceKey) -> str | None:
        """
        Return the stored link for ``ti_key``'s attempt, or None.

        The state store is read first because it is the only place an earlier attempt's link
        survives: a task's XComs are cleared before every attempt, so the XCom row holds
        whichever attempt ran last. That row is still the answer for links written before
        per-attempt rows existed.
        """
        scope = TaskScope(
            dag_id=ti_key.dag_id,
            run_id=ti_key.run_id,
            task_id=ti_key.task_id,
            map_index=ti_key.map_index,
        )
        with create_session() as session:
            stored = get_state_backend().get(
                scope, attempt_link_state_key(self.xcom_key, ti_key.try_number), session=session
            )
            if stored is not None:
                return stored

            row = session.execute(
                XComModel.get_many(
                    key=self.xcom_key,
                    run_id=ti_key.run_id,
                    dag_ids=ti_key.dag_id,
                    task_ids=ti_key.task_id,
                    map_indexes=ti_key.map_index,
                ).with_only_columns(XComModel.value)
            ).first()
        return row.value if row else None

    def get_link(self, operator: Operator, *, ti_key: TaskInstanceKey) -> str:
        """
        Retrieve the link from the XComs.

        :param operator: The Airflow operator object this link is associated to.
        :param ti_key: TaskInstance ID to return link for.
        :return: link to external system, but by pulling it from XComs
        """
        self.log.info("Attempting to retrieve link with key: %s for task id: %s", self.xcom_key, ti_key)
        raw_value = self._stored_link(ti_key)
        if raw_value is None:
            self.log.debug(
                "No link with name: %s present for key: %s, returning empty link",
                self.name,
                self.xcom_key,
            )
            return ""

        from airflow.serialization.stringify import (
            StringifyNotSupportedError,
            stringify as stringify_xcom,
        )

        try:
            parsed_value = json.loads(raw_value)
        except (ValueError, TypeError):
            # Handling for cases when types do not need to be deserialized (e.g. when value is a simple string link)
            parsed_value = raw_value

        try:
            return str(stringify_xcom(parsed_value))
        except StringifyNotSupportedError:
            # If stringify doesn't support the type, return the raw value as a string.
            # This avoids the XComModel.deserialize_value() call that could
            # instantiate arbitrary classes from untrusted XCom data.
            return str(parsed_value)
