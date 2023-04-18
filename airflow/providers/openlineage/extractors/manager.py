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

import os

from airflow.providers.openlineage.extractors import BaseExtractor, OperatorLineage
from airflow.providers.openlineage.extractors.base import DefaultExtractor
from airflow.providers.openlineage.plugins.facets import (
    UnknownOperatorAttributeRunFacet,
    UnknownOperatorInstance,
)
from airflow.utils.log.logging_mixin import LoggingMixin


class ExtractorManager(LoggingMixin):
    """Class abstracting management of custom extractors."""

    def __init__(self):
        super().__init__()
        self.extractors: dict[str, type[BaseExtractor]] = {}
        self.default_extractor = DefaultExtractor

        # Comma-separated extractors in OPENLINEAGE_EXTRACTORS variable.
        # Extractors should implement BaseExtractor
        from airflow.utils.module_loading import import_string

        # TODO: use airflow config with OL backup
        env_extractors = os.getenv("OPENLINEAGE_EXTRACTORS")
        if env_extractors is not None:
            for extractor in env_extractors.split(";"):
                extractor: type[BaseExtractor] = import_string(extractor.strip())
                for operator_class in extractor.get_operator_classnames():
                    self.extractors[operator_class] = extractor

    def add_extractor(self, operator: str, extractor: type[BaseExtractor]):
        self.extractors[operator] = extractor

    def extract_metadata(self, dagrun, task, complete: bool = False, task_instance=None) -> OperatorLineage:
        extractor = self._get_extractor(task)
        task_info = (
            f"task_type={task.task_type} "
            f"airflow_dag_id={task.dag_id} "
            f"task_id={task.task_id} "
            f"airflow_run_id={dagrun.run_id} "
        )

        if extractor:
            # Extracting advanced metadata is only possible when extractor for particular operator
            # is defined. Without it, we can't extract any input or output data.
            try:
                self.log.debug("Using extractor %s %s", extractor.__class__.__name__, str(task_info))
                if complete:
                    task_metadata = extractor.extract_on_complete(task_instance)
                else:
                    task_metadata = extractor.extract()

                self.log.debug("Found task metadata for operation %s: %s", task.task_id, str(task_metadata))
                if task_metadata:
                    if (not task_metadata.inputs) and (not task_metadata.outputs):
                        self.extract_inlets_and_outlets(task_metadata, task.inlets, task.outlets)

                    return task_metadata

            except Exception as e:
                self.log.exception(
                    "Failed to extract metadata using found extractor %s - %s %s", extractor, e, task_info
                )
        else:
            self.log.debug("Unable to find an extractor %s", task_info)

            # Only include the unkonwnSourceAttribute facet if there is no extractor
            task_metadata = OperatorLineage(
                run_facets={
                    "unknownSourceAttribute": UnknownOperatorAttributeRunFacet(
                        unknownItems=[
                            UnknownOperatorInstance(
                                name=task.task_type,
                                properties={attr: value for attr, value in task.__dict__.items()},
                            )
                        ]
                    )
                },
            )
            inlets = task.get_inlet_defs()
            outlets = task.get_outlet_defs()
            self.extract_inlets_and_outlets(task_metadata, inlets, outlets)
            return task_metadata

        return OperatorLineage()

    def _get_extractor_class(self, clazz: type) -> type[BaseExtractor] | None:
        name = clazz.__name__
        if name in self.extractors:
            return self.extractors[name]

        def method_exists(method_name):
            method = getattr(clazz, method_name, None)
            if method:
                return callable(method)

        if method_exists("get_openlineage_facets_on_start") or method_exists(
            "get_openlineage_facets_on_complete"
        ):
            return self.default_extractor
        return None

    def _get_extractor(self, task) -> BaseExtractor | None:
        # TODO: Re-enable in Extractor PR
        # self.instantiate_abstract_extractors(task)
        extractor = self._get_extractor_class(task.task_type)
        self.log.debug("extractor for %s is %s", task.__class__, extractor)
        if extractor:
            return extractor(task)
        return None

    def extract_inlets_and_outlets(
        self,
        task_metadata: OperatorLineage,
        inlets: list,
        outlets: list,
    ):
        self.log.debug("Manually extracting lineage metadata from inlets and outlets")
        for i in inlets:
            d = self.convert_to_ol_dataset(i)
            if d:
                task_metadata.inputs.append(d)
        for o in outlets:
            d = self.convert_to_ol_dataset(o)
            if d:
                task_metadata.outputs.append(d)

    @staticmethod
    def convert_to_ol_dataset(obj):
        from airflow.lineage.entities import Table
        from openlineage.client.run import Dataset

        if isinstance(obj, Dataset):
            return obj
        elif isinstance(obj, Table):
            return Dataset(
                namespace=f"{obj.cluster}",
                name=f"{obj.database}.{obj.name}",
                facets={},
            )
        else:
            return None
