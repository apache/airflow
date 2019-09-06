# -*- coding: utf-8 -*-
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
from functools import wraps

from airflow.configuration import conf
from airflow.lineage.datasets import DataSet
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.module_loading import import_string

from itertools import chain

PIPELINE_OUTLETS = "pipeline_outlets"
PIPELINE_INLETS = "pipeline_inlets"

log = LoggingMixin().log


def _get_backend():
    backend = None

    try:
        _backend_str = conf.get("lineage", "backend")
        backend = import_string(_backend_str)
    except ImportError as ie:
        log.debug("Cannot import %s due to %s", _backend_str, ie)
    except conf.AirflowConfigException:
        log.debug("Could not find lineage backend key in config")

    return backend


def apply_lineage(func):
    """
    Saves the lineage to XCom and if configured to do so sends it
    to the backend.
    """
    backend = _get_backend()

    @wraps(func)
    def wrapper(self, context, *args, **kwargs):
        self.log.debug("Backend: %s, Lineage called with inlets: %s, outlets: %s",
                       backend, self.inlets, self.outlets)
        ret_val = func(self, context, *args, **kwargs)

        outlets = [x.as_dict() for x in self.outlets]
        inlets = [x.as_dict() for x in self.inlets]

        if len(self.outlets) > 0:
            self.xcom_push(context,
                           key=PIPELINE_OUTLETS,
                           value=outlets,
                           execution_date=context['ti'].execution_date)

        if len(self.inlets) > 0:
            self.xcom_push(context,
                           key=PIPELINE_INLETS,
                           value=inlets,
                           execution_date=context['ti'].execution_date)

        if backend:
            backend.send_lineage(operator=self, inlets=self.inlets,
                                 outlets=self.outlets, context=context)

        return ret_val

    return wrapper


def prepare_lineage(func):
    """
    Prepares the lineage inlets and outlets. Inlets can be:

    * "auto" -> picks up any outlets from direct upstream tasks that have outlets defined, as such that
      if A -> B -> C and B does not have outlets but A does, these are provided as inlets.
    * "list of task_ids" -> picks up outlets from the upstream task_ids
    * "list of datasets" -> manually defined list of DataSet

    """
    @wraps(func)
    def wrapper(self, context, *args, **kwargs):
        self.log.debug("Preparing lineage inlets and outlets")

        task_ids = set(self._inlets['task_ids']).intersection(
            self.get_flat_relative_ids(upstream=True)
        )
        if task_ids:
            inlets = self.xcom_pull(context,
                                    task_ids=task_ids,
                                    dag_id=self.dag_id,
                                    key=PIPELINE_OUTLETS)
            inlets = [item for sublist in inlets if sublist for item in sublist]
            inlets = [DataSet.map_type(i['typeName'])(data=i['attributes'])
                      for i in inlets]
            self.inlets.extend(inlets)

        if self._inlets['auto']:
            # dont append twice
            task_ids = set(self._inlets['task_ids']).symmetric_difference(
                self.upstream_task_ids
            )
            inlets = self.xcom_pull(context,
                                    task_ids=task_ids,
                                    dag_id=self.dag_id,
                                    key=PIPELINE_OUTLETS)
            inlets = [item for sublist in inlets if sublist for item in sublist]
            inlets = [DataSet.map_type(i['typeName'])(data=i['attributes'])
                      for i in inlets]
            self.inlets.extend(inlets)

        if len(self._inlets['datasets']) > 0:
            self.inlets.extend(self._inlets['datasets'])

        # outlets
        if len(self._outlets['datasets']) > 0:
            self.outlets.extend(self._outlets['datasets'])

        self.log.debug("inlets: %s, outlets: %s", self.inlets, self.outlets)

        for dataset in chain(self.inlets, self.outlets):
            dataset.set_context(context)

        return func(self, context, *args, **kwargs)

    return wrapper
