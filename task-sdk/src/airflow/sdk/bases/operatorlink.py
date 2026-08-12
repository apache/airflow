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

if TYPE_CHECKING:
    from airflow.sdk import BaseOperator
    from airflow.sdk.types import TaskInstanceKey


ATTEMPT_LINK_XCOM_KEY_PREFIX = "_link_attempt_"
"""Prefix marking the XCom rows that hold one attempt's rendered operator link."""


def attempt_link_xcom_key(xcom_key: str, try_number: int) -> str:
    """Return the XCom key holding ``xcom_key``'s link as rendered for ``try_number``."""
    return f"{ATTEMPT_LINK_XCOM_KEY_PREFIX}{try_number}_{xcom_key}"


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

    keeps_a_link_per_attempt: ClassVar[bool] = False
    """
    Keep one row per attempt rather than a single row holding whichever ran last.

    Set on links whose URL cannot be recomputed, such as one carrying a job id the remote
    service mints per submission. A task's XComs are cleared before each attempt; the rows
    behind a link that sets this are kept.
    """

    def xcom_key_for_try(self, try_number: int) -> str:
        """Return the XCom key holding this link as rendered for ``try_number``."""
        if not self.keeps_a_link_per_attempt:
            return self.xcom_key
        return attempt_link_xcom_key(self.xcom_key, try_number)

    @abstractmethod
    def get_link(self, operator: BaseOperator, *, ti_key: TaskInstanceKey) -> str:
        """
        Link to external system.

        :param operator: The Airflow operator object this link is associated to.
        :param ti_key: TaskInstance ID to return link for.
        :return: link to external system
        """
