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
"""Provides lineage support functions."""

from __future__ import annotations

import logging
from functools import wraps
from typing import TYPE_CHECKING, Any, Callable, TypeVar, cast

from airflow.configuration import conf
from airflow.lineage.backend import LineageBackend
from airflow.utils.session import create_session

if TYPE_CHECKING:
    from airflow.utils.context import Context

PIPELINE_OUTLETS = "pipeline_outlets"
PIPELINE_INLETS = "pipeline_inlets"
AUTO = "auto"

log = logging.getLogger(__name__)


def get_backend() -> LineageBackend | None:
    """Get the lineage backend if defined in the configs."""
    clazz = conf.getimport("lineage", "backend", fallback=None)

    if clazz:
        if not issubclass(clazz, LineageBackend):
            raise TypeError(
                f"Your custom Lineage class `{clazz.__name__}` "
                f"is not a subclass of `{LineageBackend.__name__}`."
            )
        else:
            return clazz()

    return None


def _render_object(obj: Any, context: Context) -> dict:
    ti = context["ti"]
    if TYPE_CHECKING:
        assert ti.task
    return ti.task.render_template(obj, context)


T = TypeVar("T", bound=Callable)


def apply_lineage(func: T) -> T:
    """
    Conditionally send lineage to the backend.

    Saves the lineage to XCom and if configured to do so sends it
    to the backend.
    """
    _backend = get_backend()

    @wraps(func)
    def wrapper(self, context, *args, **kwargs):
        self.log.debug("Lineage called with inlets: %s, outlets: %s", self.inlets, self.outlets)

        ret_val = func(self, context, *args, **kwargs)

        outlets = list(self.outlets)
        inlets = list(self.inlets)

        if outlets:
            self.xcom_push(context, key=PIPELINE_OUTLETS, value=outlets)

        if inlets:
            self.xcom_push(context, key=PIPELINE_INLETS, value=inlets)

        if _backend:
            _backend.send_lineage(operator=self, inlets=self.inlets, outlets=self.outlets, context=context)

        return ret_val

    return cast(T, wrapper)


def prepare_lineage(func: T) -> T:
    """
    Prepare the lineage inlets and outlets.

    Inlets can be:

    * "auto" -> picks up any outlets from direct upstream tasks that have outlets defined, as such that
      if A -> B -> C and B does not have outlets but A does, these are provided as inlets.
    * "list of task_ids" -> picks up outlets from the upstream task_ids
    * "list of datasets" -> manually defined list of dataset

    """

    @wraps(func)
    def wrapper(self, context, *args, **kwargs):
        from airflow.models.abstractoperator import AbstractOperator

        self.log.debug("Preparing lineage inlets and outlets")

        if isinstance(self.inlets, (str, AbstractOperator)):
            self.inlets = [self.inlets]

        if self.inlets and isinstance(self.inlets, list):
            # get task_ids that are specified as parameter and make sure they are upstream
            task_ids = {o for o in self.inlets if isinstance(o, str)}.union(
                op.task_id for op in self.inlets if isinstance(op, AbstractOperator)
            ).intersection(self.get_flat_relative_ids(upstream=True))

            # pick up unique direct upstream task_ids if AUTO is specified
            if AUTO.upper() in self.inlets or AUTO.lower() in self.inlets:
                task_ids = task_ids.union(task_ids.symmetric_difference(self.upstream_task_ids))

            # Remove auto and task_ids
            self.inlets = [i for i in self.inlets if not isinstance(i, str)]

            # We manually create a session here since xcom_pull returns a
            # LazySelectSequence proxy. If we do not pass a session, a new one
            # will be created, but that session will not be properly closed.
            # After we are done iterating, we can safely close this session.
            with create_session() as session:
                _inlets = self.xcom_pull(
                    context, task_ids=task_ids, dag_id=self.dag_id, key=PIPELINE_OUTLETS, session=session
                )
                self.inlets.extend(i for it in _inlets for i in it)

        elif self.inlets:
            raise AttributeError("inlets is not a list, operator, string or attr annotated object")

        if not isinstance(self.outlets, list):
            self.outlets = [self.outlets]

        # render inlets and outlets
        self.inlets = [_render_object(i, context) for i in self.inlets]

        self.outlets = [_render_object(i, context) for i in self.outlets]

        self.log.debug("inlets: %s, outlets: %s", self.inlets, self.outlets)

        return func(self, context, *args, **kwargs)

    return cast(T, wrapper)
