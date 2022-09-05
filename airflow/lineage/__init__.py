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
"""Provides lineage support functions"""
import itertools
import logging
from functools import wraps
from typing import TYPE_CHECKING, Any, Callable, List, Optional, TypeVar, cast

import attr

from airflow.configuration import conf
from airflow.lineage.backend import LineageBackend
from airflow.utils.module_loading import import_string

if TYPE_CHECKING:
    from airflow.utils.context import Context


PIPELINE_OUTLETS = "pipeline_outlets"
PIPELINE_INLETS = "pipeline_inlets"
AUTO = "auto"

log = logging.getLogger(__name__)


def get_backend() -> Optional[LineageBackend]:
    """Gets the lineage backend if defined in the configs"""
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


def _render_object(obj: Any, context: "Context") -> dict:
    return context['ti'].task.render_template(obj, context)


def _deserialize(serialized: dict):
    from airflow.serialization.serialized_objects import BaseSerialization

    # This is only use in the worker side, so it is okay to "blindly" import the specified class here.
    cls = import_string(serialized['__type'])
    return cls(**BaseSerialization.deserialize(serialized['__var']))


def _serialize(objs: List[Any], source: str):
    """Serialize an attrs-decorated class to JSON"""
    from airflow.serialization.serialized_objects import BaseSerialization

    for obj in objs:
        if not attr.has(obj):
            continue

        type_name = obj.__module__ + '.' + obj.__class__.__name__
        # Only include attributes which we can pass back to the classes constructor
        data = attr.asdict(obj, recurse=True, filter=lambda a, v: a.init)

        yield {
            k: BaseSerialization.serialize(v)
            for k, v in (
                ('__type', type_name),
                ('__source', source),
                ('__var', data),
            )
        }


T = TypeVar("T", bound=Callable)


def apply_lineage(func: T) -> T:
    """
    Saves the lineage to XCom and if configured to do so sends it
    to the backend.
    """
    _backend = get_backend()

    @wraps(func)
    def wrapper(self, context, *args, **kwargs):

        self.log.debug("Lineage called with inlets: %s, outlets: %s", self.inlets, self.outlets)
        ret_val = func(self, context, *args, **kwargs)

        outlets = list(_serialize(self.outlets, f"{self.dag_id}.{self.task_id}"))
        inlets = list(_serialize(self.inlets, None))

        if outlets:
            self.xcom_push(
                context, key=PIPELINE_OUTLETS, value=outlets, execution_date=context['ti'].execution_date
            )

        if inlets:
            self.xcom_push(
                context, key=PIPELINE_INLETS, value=inlets, execution_date=context['ti'].execution_date
            )

        if _backend:
            _backend.send_lineage(operator=self, inlets=self.inlets, outlets=self.outlets, context=context)

        return ret_val

    return cast(T, wrapper)


def prepare_lineage(func: T) -> T:
    """
    Prepares the lineage inlets and outlets. Inlets can be:

    * "auto" -> picks up any outlets from direct upstream tasks that have outlets defined, as such that
      if A -> B -> C and B does not have outlets but A does, these are provided as inlets.
    * "list of task_ids" -> picks up outlets from the upstream task_ids
    * "list of datasets" -> manually defined list of data

    """

    @wraps(func)
    def wrapper(self, context, *args, **kwargs):
        from airflow.models.abstractoperator import AbstractOperator

        self.log.debug("Preparing lineage inlets and outlets")

        if isinstance(self.inlets, (str, AbstractOperator)):
            self.inlets = [self.inlets]

        if self.inlets and isinstance(self.inlets, list):
            # get task_ids that are specified as parameter and make sure they are upstream
            task_ids = (
                {o for o in self.inlets if isinstance(o, str)}
                .union(op.task_id for op in self.inlets if isinstance(op, AbstractOperator))
                .intersection(self.get_flat_relative_ids(upstream=True))
            )

            # pick up unique direct upstream task_ids if AUTO is specified
            if AUTO.upper() in self.inlets or AUTO.lower() in self.inlets:
                task_ids = task_ids.union(task_ids.symmetric_difference(self.upstream_task_ids))

            # Remove auto and task_ids
            self.inlets = [i for i in self.inlets if not isinstance(i, str)]
            _inlets = self.xcom_pull(context, task_ids=task_ids, dag_id=self.dag_id, key=PIPELINE_OUTLETS)

            # re-instantiate the obtained inlets
            # xcom_pull returns a list of items for each given task_id
            _inlets = [_deserialize(item) for item in itertools.chain.from_iterable(_inlets)]

            self.inlets.extend(_inlets)

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
