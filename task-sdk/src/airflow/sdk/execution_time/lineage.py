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
from functools import cache
from typing import TYPE_CHECKING

from airflow.sdk.definitions.lineage import LineageBackend

if TYPE_CHECKING:
    from structlog.typing import FilteringBoundLogger as Logger

    from airflow.sdk.definitions.context import Context

PIPELINE_OUTLETS = "pipeline_outlets"
PIPELINE_INLETS = "pipeline_inlets"
AUTO = "auto"


@cache
def _get_backend() -> LineageBackend | None:
    """Get the lineage backend if defined in the configs."""
    from airflow.configuration import conf

    if not (klass := conf.getimport("lineage", "backend", fallback=None)):
        return None
    if issubclass(klass, LineageBackend):
        return klass()
    raise TypeError(
        f"Your custom Lineage class `{klass.__name__}` is not a subclass of `{LineageBackend.__name__}`."
    )


def apply_lineage(context: Context, log: Logger | logging.Logger) -> None:
    """
    Conditionally send lineage to the backend.

    Saves the lineage to XCom and if configured to do so sends it to the backend.
    """
    backend = _get_backend()
    ti = context["ti"]
    task = ti.task

    if isinstance(log, logging.Logger):
        log.debug("Applying inlets: %s outlets: %s", inlets := task.inlets, outlets := task.outlets)
    else:
        log.debug("Applying lineage", inlets=(inlets := task.inlets), outlets=(outlets := task.outlets))

    if outlets:
        ti.xcom_push(PIPELINE_OUTLETS, outlets)
    if inlets:
        ti.xcom_push(PIPELINE_INLETS, inlets)
    if backend:
        backend.send_lineage(operator=task, inlets=inlets, outlets=outlets, context=context)


def prepare_lineage(context: Context, log: Logger | logging.Logger) -> None:
    """
    Prepare the lineage inlets and outlets.

    Inlets can be:

    * "auto" -> picks up any outlets from direct upstream tasks that have outlets
      defined, as such that if A -> B -> C and B does not have outlets but A does,
      these are provided as inlets.
    * "list of task_ids" -> picks up outlets from the upstream task_ids
    * "list of datasets" -> manually defined list of dataset
    """
    from airflow.sdk.definitions._internal.abstractoperator import AbstractOperator

    log.debug("Preparing lineage inlets and outlets")
    ti = context["ti"]
    task = ti.task

    if isinstance(task.inlets, (str, AbstractOperator)):
        task.inlets = [task.inlets]
    if not isinstance(task.inlets, list):
        raise AttributeError("inlets is not a list, operator, string or attr annotated object")

    # get task_ids that are specified as parameter and make sure they are upstream
    task_ids = {o for o in task.inlets if isinstance(o, str)}.union(
        op.task_id for op in task.inlets if isinstance(op, AbstractOperator)
    ).intersection(task.get_flat_relative_ids(upstream=True))

    # pick up unique direct upstream task_ids if AUTO is specified
    if AUTO.upper() in task.inlets or AUTO.lower() in task.inlets:
        task_ids = task_ids.union(task_ids.symmetric_difference(task.upstream_task_ids))

    # Remove auto and task_ids
    task.inlets = [i for i in task.inlets if not isinstance(i, str)]

    # Inject upstream outlets
    if task_ids:
        task.inlets.extend(i for it in ti.xcom_pull(task_ids=task_ids, key=PIPELINE_OUTLETS) for i in it)

    if task.outlets and not isinstance(task.outlets, list):
        task.outlets = [task.outlets]

    # render inlets and outlets
    task.inlets = [task.render_template(obj, context) for obj in task.inlets]
    task.outlets = [task.render_template(obj, context) for obj in task.outlets]

    if isinstance(log, logging.Logger):
        log.debug("Rendered inlets: %s, outlets: %s", task.inlets, task.outlets)
    else:
        log.debug("Rendered lineage objects", inlets=task.inlets, outlets=task.outlets)
