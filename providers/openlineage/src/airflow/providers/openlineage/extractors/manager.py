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

from collections.abc import Iterator
from typing import TYPE_CHECKING

from airflow.providers.common.compat.openlineage.utils.utils import (
    translate_airflow_asset,
)
from airflow.providers.openlineage import conf
from airflow.providers.openlineage.extractors import BaseExtractor, OperatorLineage
from airflow.providers.openlineage.extractors.base import (
    OL_METHOD_NAME_COMPLETE,
    OL_METHOD_NAME_START,
    DefaultExtractor,
)
from airflow.providers.openlineage.extractors.bash import BashExtractor
from airflow.providers.openlineage.extractors.python import PythonExtractor
from airflow.providers.openlineage.utils.utils import (
    get_unknown_source_attribute_run_facet,
    try_import_from_string,
)
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import TaskInstanceState

if TYPE_CHECKING:
    from openlineage.client.event_v2 import Dataset

    from airflow.providers.common.compat.lineage.entities import Table
    from airflow.providers.common.compat.sdk import BaseOperator


def _iter_extractor_types() -> Iterator[type[BaseExtractor]]:
    if PythonExtractor is not None:
        yield PythonExtractor
    if BashExtractor is not None:
        yield BashExtractor


class ExtractorManager(LoggingMixin):
    """Class abstracting management of custom extractors."""

    def __init__(self):
        super().__init__()
        self.extractors: dict[str, type[BaseExtractor]] = {}
        self.default_extractor = DefaultExtractor

        # Built-in Extractors like Bash and Python
        for extractor in _iter_extractor_types():
            for operator_class in extractor.get_operator_classnames():
                self.extractors[operator_class] = extractor

        for extractor_path in conf.custom_extractors():
            extractor: type[BaseExtractor] | None = try_import_from_string(extractor_path)
            if not extractor:
                self.log.warning(
                    "OpenLineage is unable to import custom extractor `%s`; will ignore it.",
                    extractor_path,
                )
                continue
            for operator_class in extractor.get_operator_classnames():
                if operator_class in self.extractors:
                    self.log.warning(
                        "Duplicate OpenLineage custom extractor found for `%s`. "
                        "`%s` will be used instead of `%s`",
                        operator_class,
                        extractor_path,
                        self.extractors[operator_class],
                    )
                self.extractors[operator_class] = extractor
                self.log.debug(
                    "Registered custom OpenLineage extractor `%s` for class `%s`",
                    extractor_path,
                    operator_class,
                )

    def add_extractor(self, operator_class: str, extractor: type[BaseExtractor]):
        self.extractors[operator_class] = extractor

    def extract_metadata(
        self, dagrun, task, task_instance_state: TaskInstanceState, task_instance=None
    ) -> OperatorLineage:
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
                self.log.debug(
                    "Using extractor %s %s",
                    extractor.__class__.__name__,
                    str(task_info),
                )
                if task_instance_state == TaskInstanceState.RUNNING:
                    task_metadata = extractor.extract()
                elif task_instance_state == TaskInstanceState.FAILED:
                    if callable(getattr(extractor, "extract_on_failure", None)):
                        task_metadata = extractor.extract_on_failure(task_instance)
                    else:
                        task_metadata = extractor.extract_on_complete(task_instance)
                else:
                    task_metadata = extractor.extract_on_complete(task_instance)

                self.log.debug(
                    "Found task metadata for operation %s: %s",
                    task.task_id,
                    str(task_metadata),
                )
                task_metadata = self.validate_task_metadata(task_metadata)
                if task_metadata:
                    if (not task_metadata.inputs) and (not task_metadata.outputs):
                        if (hook_lineage := self.get_hook_lineage()) is not None:
                            inputs, outputs = hook_lineage
                            task_metadata.inputs = inputs
                            task_metadata.outputs = outputs
                        else:
                            self.extract_inlets_and_outlets(task_metadata, task)
                    return task_metadata

            except Exception as e:
                self.log.warning(
                    "Failed to extract metadata using found extractor %s - %s %s",
                    extractor,
                    e,
                    task_info,
                )
                self.log.debug("OpenLineage extraction failure details:", exc_info=True)
        elif (hook_lineage := self.get_hook_lineage()) is not None:
            inputs, outputs = hook_lineage
            task_metadata = OperatorLineage(inputs=inputs, outputs=outputs)
            return task_metadata
        else:
            self.log.debug("Unable to find an extractor %s", task_info)

            # Only include the unkonwnSourceAttribute facet if there is no extractor
            task_metadata = OperatorLineage(
                run_facets=get_unknown_source_attribute_run_facet(task=task),
            )
            self.extract_inlets_and_outlets(task_metadata, task)
            return task_metadata

        return OperatorLineage()

    def get_extractor_class(self, task: BaseOperator) -> type[BaseExtractor] | None:
        if task.task_type in self.extractors:
            return self.extractors[task.task_type]

        def method_exists(method_name):
            return callable(getattr(task, method_name, None))

        if method_exists(OL_METHOD_NAME_START) or method_exists(OL_METHOD_NAME_COMPLETE):
            return self.default_extractor
        return None

    def _get_extractor(self, task: BaseOperator) -> BaseExtractor | None:
        # TODO: Re-enable in Extractor PR
        # self.instantiate_abstract_extractors(task)
        extractor = self.get_extractor_class(task)
        self.log.debug("extractor for %s is %s", task.task_type, extractor)
        if extractor:
            return extractor(task)
        return None

    def extract_inlets_and_outlets(self, task_metadata: OperatorLineage, task) -> None:
        if task.inlets or task.outlets:
            self.log.debug("Manually extracting lineage metadata from inlets and outlets")
        for i in task.inlets:
            d = self.convert_to_ol_dataset(i)
            if d:
                task_metadata.inputs.append(d)
        for o in task.outlets:
            d = self.convert_to_ol_dataset(o)
            if d:
                task_metadata.outputs.append(d)

    def get_hook_lineage(self) -> tuple[list[Dataset], list[Dataset]] | None:
        try:
            from airflow.providers.common.compat.lineage.hook import get_hook_lineage_collector
        except ImportError:
            return None

        if not hasattr(get_hook_lineage_collector(), "has_collected"):
            return None
        if not get_hook_lineage_collector().has_collected:
            return None

        self.log.debug("OpenLineage will extract lineage from Hook Lineage Collector.")
        return (
            [
                asset
                for asset_info in get_hook_lineage_collector().collected_assets.inputs
                if (asset := translate_airflow_asset(asset_info.asset, asset_info.context)) is not None
            ],
            [
                asset
                for asset_info in get_hook_lineage_collector().collected_assets.outputs
                if (asset := translate_airflow_asset(asset_info.asset, asset_info.context)) is not None
            ],
        )

    @staticmethod
    def convert_to_ol_dataset_from_object_storage_uri(uri: str) -> Dataset | None:
        from urllib.parse import urlparse

        from openlineage.client.event_v2 import Dataset

        if "/" not in uri:
            return None

        try:
            scheme, netloc, path, params, _, _ = urlparse(uri)
        except Exception:
            return None

        common_schemas = {
            "s3": "s3",
            "gs": "gs",
            "gcs": "gs",
            "hdfs": "hdfs",
            "file": "file",
        }
        for found, final in common_schemas.items():
            if scheme.startswith(found):
                return Dataset(namespace=f"{final}://{netloc}", name=path.lstrip("/"))
        return Dataset(namespace=scheme, name=f"{netloc}{path}")

    @staticmethod
    def convert_to_ol_dataset_from_table(table: Table) -> Dataset:
        from openlineage.client.event_v2 import Dataset
        from openlineage.client.facet_v2 import (
            DatasetFacet,
            documentation_dataset,
            ownership_dataset,
            schema_dataset,
        )

        facets: dict[str, DatasetFacet] = {}
        if table.columns:
            facets["schema"] = schema_dataset.SchemaDatasetFacet(
                fields=[
                    schema_dataset.SchemaDatasetFacetFields(
                        name=column.name,
                        type=column.data_type,
                        description=column.description,
                    )
                    for column in table.columns
                ]
            )
        if table.owners:
            facets["ownership"] = ownership_dataset.OwnershipDatasetFacet(
                owners=[
                    ownership_dataset.Owner(
                        # f.e. "user:John Doe <jdoe@company.com>" or just "user:<jdoe@company.com>"
                        name=f"user:"
                        f"{user.first_name + ' ' if user.first_name else ''}"
                        f"{user.last_name + ' ' if user.last_name else ''}"
                        f"<{user.email}>",
                        type="",
                    )
                    for user in table.owners
                ]
            )
        if table.description:
            facets["documentation"] = documentation_dataset.DocumentationDatasetFacet(
                description=table.description
            )
        return Dataset(
            namespace=f"{table.cluster}",
            name=f"{table.database}.{table.name}",
            facets=facets,
        )

    @staticmethod
    def convert_to_ol_dataset(obj) -> Dataset | None:
        from openlineage.client.event_v2 import Dataset

        from airflow.providers.common.compat.lineage.entities import File, Table

        if isinstance(obj, Dataset):
            return obj
        if isinstance(obj, Table):
            return ExtractorManager.convert_to_ol_dataset_from_table(obj)
        if isinstance(obj, File):
            return ExtractorManager.convert_to_ol_dataset_from_object_storage_uri(obj.url)
        return None

    def validate_task_metadata(self, task_metadata) -> OperatorLineage | None:
        try:
            return OperatorLineage(
                inputs=task_metadata.inputs,
                outputs=task_metadata.outputs,
                run_facets=task_metadata.run_facets,
                job_facets=task_metadata.job_facets,
            )
        except AttributeError:
            self.log.warning("OpenLineage extractor returns non-valid metadata: `%s`", task_metadata)
            return None
