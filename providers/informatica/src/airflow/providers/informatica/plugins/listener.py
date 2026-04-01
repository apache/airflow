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

import logging
from typing import TYPE_CHECKING

from airflow.listeners import hookimpl
from airflow.providers.informatica.extractors import InformaticaLineageExtractor
from airflow.providers.informatica.hooks.edc import InformaticaEDCHook

if TYPE_CHECKING:
    from airflow.models import TaskInstance
    from airflow.utils.state import TaskInstanceState

_informatica_listener: InformaticaListener | None = None


class InformaticaListener:
    """Informatica listener sends events on task instance state changes to Informatica EDC for lineage tracking."""

    def __init__(self):
        self._executor = None
        self.log = logging.getLogger(__name__)
        self.hook = InformaticaLineageExtractor(edc_hook=InformaticaEDCHook())
        # self.extractor_manager = ExtractorManager()

    @hookimpl
    def on_task_instance_success(
        self, previous_state: TaskInstanceState, task_instance: TaskInstance, *args, **kwargs
    ):
        self._handle_lineage(task_instance, state="success")

    @hookimpl
    def on_task_instance_failed(
        self, previous_state: TaskInstanceState, task_instance: TaskInstance, *args, **kwargs
    ):
        self._handle_lineage(task_instance, state="failed")

    @hookimpl
    def on_task_instance_running(
        self, previous_state: TaskInstanceState, task_instance: TaskInstance, *args, **kwargs
    ):
        self._handle_lineage(task_instance, state="running")

    def _handle_lineage(self, task_instance: TaskInstance, state: str):
        """
        Handle lineage resolution for inlets and outlets.

        For each inlet and outlet, resolve Informatica EDC object IDs using getObject.
        If valid, collect and create lineage links between all valid inlets and outlets.
        """
        task = getattr(task_instance, "task", None)
        if not task:
            self.log.debug("No task found for TaskInstance %s", task_instance)
            return
        inlets = getattr(task, "inlets", getattr(task_instance, "inlets", []))
        outlets = getattr(task, "outlets", getattr(task_instance, "outlets", []))

        valid_inlets = []  # List of tuples: (uri, object_id)
        valid_outlets = []

        self.log.info("[InformaticaLineageListener] Task: %s State: %s", task_instance.task_id, state)

        if state != "success":
            self.log.info("[InformaticaLineageListener] Skipping lineage handling for state: %s", state)
            return

        for inlet in inlets:
            inlet_uri = None
            if isinstance(inlet, dict) and "dataset_uri" in inlet:
                inlet_uri = inlet["dataset_uri"]
            elif isinstance(inlet, str):
                inlet_uri = inlet
            else:
                self.log.error("Inlet is not a string or dict with 'dataset_uri': %s", inlet)
                continue
            self.log.info("[InformaticaLineageListener] Inlet: %s and type: %s", inlet_uri, type(inlet))
            try:
                obj = self.hook.get_object(inlet_uri)
                if obj and "id" in obj and obj["id"]:
                    valid_inlets.append((inlet_uri, obj["id"]))
            except Exception as e:
                self.log.exception("Failed to resolve inlet %s: %s", inlet_uri, e)

        for outlet in outlets:
            outlet_uri = None
            if isinstance(outlet, dict) and "dataset_uri" in outlet:
                outlet_uri = outlet["dataset_uri"]
            elif isinstance(outlet, str):
                outlet_uri = outlet
            else:
                self.log.error("Outlet is not a string or dict with 'dataset_uri': %s", outlet)
                continue
            self.log.info("[InformaticaLineageListener] Outlet: %s", outlet_uri)
            try:
                obj = self.hook.get_object(outlet_uri)
                if obj and "id" in obj and obj["id"]:
                    valid_outlets.append((outlet_uri, obj["id"]))
            except Exception as e:
                self.log.warning("Failed to resolve outlet %s: %s", outlet_uri, e)

        # Create lineage links between all valid inlet and outlet object IDs
        for inlet_uri, inlet_id in valid_inlets:
            for outlet_uri, outlet_id in valid_outlets:
                try:
                    self.log.info(
                        "[InformaticaLineageListener] Creating lineage link: %s (%s) -> %s (%s)",
                        inlet_uri,
                        inlet_id,
                        outlet_uri,
                        outlet_id,
                    )
                    result = self.hook.create_lineage_link(inlet_id, outlet_id)
                    self.log.info("Lineage link created: %s -> %s | Result: %s", inlet_id, outlet_id, result)
                except Exception as e:
                    self.log.exception(
                        "Failed to create lineage link from %s to %s: %s", inlet_id, outlet_id, e
                    )


def get_informatica_listener() -> InformaticaListener:
    """Get singleton listener manager."""
    global _informatica_listener
    if not _informatica_listener:
        _informatica_listener = InformaticaListener()
    return _informatica_listener
