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
import json
import logging
from functools import wraps
from typing import Any, Callable, Dict, Optional, TypeVar, cast

import attr
import jinja2
from cattr import structure, unstructure

from airflow.configuration import conf
from airflow.lineage.backend import LineageBackend
from airflow.utils.module_loading import import_string

ENV = jinja2.Environment()

PIPELINE_OUTLETS = "pipeline_outlets"
PIPELINE_INLETS = "pipeline_inlets"
AUTO = "auto"

log = logging.getLogger(__name__)


@attr.s(auto_attribs=True)
class Metadata:
    """Class for serialized entities."""

    type_name: str = attr.ib()
    source: str = attr.ib()
    data: Dict = attr.ib()


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


def _get_instance(meta: Metadata):
    """Instantiate an object from Metadata"""
    cls = import_string(meta.type_name)
    return structure(meta.data, cls)


def _render_object(obj: Any, context) -> Any:
    """Renders a attr annotated object. Will set non serializable attributes to none"""
    return structure(
        json.loads(
            ENV.from_string(json.dumps(unstructure(obj), default=lambda o: None))
            .render(**context)
            .encode('utf-8')
        ),
        type(obj),
    )


def _to_dataset(obj: Any, source: str) -> Optional[Metadata]:
    """Create Metadata from attr annotated object"""
    if not attr.has(obj):
        return None

    type_name = obj.__module__ + '.' + obj.__class__.__name__
    data = unstructure(obj)

    return Metadata(type_name, source, data)


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

        outlets = [unstructure(_to_dataset(x, f"{self.dag_id}.{self.task_id}")) for x in self.outlets]
        inlets = [unstructure(_to_dataset(x, None)) for x in self.inlets]

        if self.outlets:
            self.xcom_push(
                context, key=PIPELINE_OUTLETS, value=outlets, execution_date=context['ti'].execution_date
            )

        if self.inlets:
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

        if isinstance(self._inlets, (str, AbstractOperator)) or attr.has(self._inlets):
            self._inlets = [
                self._inlets,
            ]

        if self._inlets and isinstance(self._inlets, list):
            # get task_ids that are specified as parameter and make sure they are upstream
            task_ids = (
                {o for o in self._inlets if isinstance(o, str)}
                .union(op.task_id for op in self._inlets if isinstance(op, AbstractOperator))
                .intersection(self.get_flat_relative_ids(upstream=True))
            )

            # pick up unique direct upstream task_ids if AUTO is specified
            if AUTO.upper() in self._inlets or AUTO.lower() in self._inlets:
                task_ids = task_ids.union(task_ids.symmetric_difference(self.upstream_task_ids))

            _inlets = self.xcom_pull(context, task_ids=task_ids, dag_id=self.dag_id, key=PIPELINE_OUTLETS)

            # re-instantiate the obtained inlets
            _inlets = [
                _get_instance(structure(item, Metadata)) for sublist in _inlets if sublist for item in sublist
            ]

            self.inlets.extend(_inlets)
            self.inlets.extend(self._inlets)

        elif self._inlets:
            raise AttributeError("inlets is not a list, operator, string or attr annotated object")

        if not isinstance(self._outlets, list):
            self._outlets = [
                self._outlets,
            ]

        self.outlets.extend(self._outlets)

        # render inlets and outlets
        self.inlets = [_render_object(i, context) for i in self.inlets if attr.has(i)]

        self.outlets = [_render_object(i, context) for i in self.outlets if attr.has(i)]

        self.log.debug("inlets: %s, outlets: %s", self.inlets, self.outlets)
        return func(self, context, *args, **kwargs)

    return cast(T, wrapper)
