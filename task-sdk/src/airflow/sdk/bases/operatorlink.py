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

    @property
    def static_url(self) -> str | None:
        """
        Constant URL for this link, when it does not depend on the task run.

        Links that override this property don't need an XCom entry: the URL is
        carried through DAG serialization and rendered directly by the API
        server, even for task instances that never pushed it via ``get_link()``.

        Defaults to ``None``, meaning the link is resolved dynamically through
        :meth:`get_link` and stored in XComs as before. Set it (instead of
        overriding :meth:`get_link`) for links whose URL is always the same.
        """
        return None

    def get_link(self, operator: BaseOperator, *, ti_key: TaskInstanceKey) -> str:
        """
        Link to external system.

        Subclasses whose URL does not depend on the task run may override
        :attr:`static_url` instead of this method and skip the XCom round-trip.

        :param operator: The Airflow operator object this link is associated to.
        :param ti_key: TaskInstance ID to return link for.
        :return: link to external system
        """
        if self.static_url is not None:
            return self.static_url
        raise NotImplementedError(
            f"Operator link {type(self).__name__} must override get_link() or static_url to provide a URL"
        )

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        def _overridden(attr: str) -> bool:
            return any(attr in c.__dict__ for c in cls.__mro__ if c is not BaseOperatorLink)

        # ``name`` is the only @abstractmethod. A class stays abstract exactly
        # when some abstract member of the MRO is not overridden by a concrete
        # member below it — deliberate intermediate bases rely on that, and
        # ABCMeta computes ``__abstractmethods__`` only after
        # ``__init_subclass__`` runs, so abstractness is re-derived here.
        abstract_names = {
            attr
            for c in cls.__mro__
            for attr, member in c.__dict__.items()
            if getattr(member, "__isabstractmethod__", False)
        }
        concrete_chain_names = {
            attr
            for c in cls.__mro__
            if c not in (BaseOperatorLink, object)
            for attr, member in c.__dict__.items()
            if not getattr(member, "__isabstractmethod__", False)
        }
        if abstract_names - concrete_chain_names:
            return
        if not (_overridden("get_link") or _overridden("static_url")):
            raise TypeError(
                f"Operator link {cls.__name__} must override get_link() or static_url; "
                "inheriting both defers the failure to link-resolution time, after the task has run"
            )
