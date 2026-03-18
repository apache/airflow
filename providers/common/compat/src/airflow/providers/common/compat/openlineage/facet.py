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

from typing import TYPE_CHECKING


def create_no_op(*_, **__) -> None:
    """
    Create a no-op placeholder.

    This function creates and returns a None value, used as a placeholder when the OpenLineage client
    library is available. It represents an action that has no effect.
    """
    return None


if TYPE_CHECKING:
    from openlineage.client.generated.base import (
        BaseFacet,
        Dataset,
        DatasetFacet,
        InputDataset,
        OutputDataset,
        RunFacet,
    )
    from openlineage.client.generated.column_lineage_dataset import (
        ColumnLineageDatasetFacet,
        Fields,
        InputField,
    )
    from openlineage.client.generated.documentation_dataset import DocumentationDatasetFacet
    from openlineage.client.generated.error_message_run import ErrorMessageRunFacet
    from openlineage.client.generated.external_query_run import ExternalQueryRunFacet
    from openlineage.client.generated.extraction_error_run import Error, ExtractionErrorRunFacet
    from openlineage.client.generated.lifecycle_state_change_dataset import (
        LifecycleStateChange,
        LifecycleStateChangeDatasetFacet,
        PreviousIdentifier,
    )
    from openlineage.client.generated.output_statistics_output_dataset import (
        OutputStatisticsOutputDatasetFacet,
    )
    from openlineage.client.generated.schema_dataset import SchemaDatasetFacet, SchemaDatasetFacetFields
    from openlineage.client.generated.sql_job import SQLJobFacet
    from openlineage.client.generated.symlinks_dataset import Identifier, SymlinksDatasetFacet
else:
    try:
        try:
            from openlineage.client.generated.base import (
                BaseFacet,
                Dataset,
                DatasetFacet,
                InputDataset,
                OutputDataset,
                RunFacet,
            )
            from openlineage.client.generated.column_lineage_dataset import (
                ColumnLineageDatasetFacet,
                Fields,
                InputField,
            )
            from openlineage.client.generated.documentation_dataset import DocumentationDatasetFacet
            from openlineage.client.generated.error_message_run import ErrorMessageRunFacet
            from openlineage.client.generated.external_query_run import ExternalQueryRunFacet
            from openlineage.client.generated.extraction_error_run import Error, ExtractionErrorRunFacet
            from openlineage.client.generated.lifecycle_state_change_dataset import (
                LifecycleStateChange,
                LifecycleStateChangeDatasetFacet,
                PreviousIdentifier,
            )
            from openlineage.client.generated.output_statistics_output_dataset import (
                OutputStatisticsOutputDatasetFacet,
            )
            from openlineage.client.generated.schema_dataset import (
                SchemaDatasetFacet,
                SchemaDatasetFacetFields,
            )
            from openlineage.client.generated.sql_job import SQLJobFacet
            from openlineage.client.generated.symlinks_dataset import Identifier, SymlinksDatasetFacet
        except ImportError:
            from openlineage.client.facet import (
                BaseFacet,
                BaseFacet as DatasetFacet,
                BaseFacet as RunFacet,
                ColumnLineageDatasetFacet,
                ColumnLineageDatasetFacetFieldsAdditional as Fields,
                ColumnLineageDatasetFacetFieldsAdditionalInputFields as InputField,
                DocumentationDatasetFacet,
                ErrorMessageRunFacet,
                ExternalQueryRunFacet,
                ExtractionError as Error,
                ExtractionErrorRunFacet,
                LifecycleStateChange,
                LifecycleStateChangeDatasetFacet,
                LifecycleStateChangeDatasetFacetPreviousIdentifier as PreviousIdentifier,
                OutputStatisticsOutputDatasetFacet,
                SchemaDatasetFacet,
                SchemaField as SchemaDatasetFacetFields,
                SqlJobFacet as SQLJobFacet,
                SymlinksDatasetFacet,
                SymlinksDatasetFacetIdentifiers as Identifier,
            )
            from openlineage.client.run import Dataset, InputDataset, OutputDataset
    except ImportError:
        # When no openlineage client library installed we create no-op classes.
        # This allows avoiding raising ImportError when making OL imports in top-level code
        # (which shouldn't be the case anyway).
        BaseFacet = Dataset = DatasetFacet = InputDataset = OutputDataset = RunFacet = (
            ColumnLineageDatasetFacet
        ) = Fields = InputField = DocumentationDatasetFacet = ErrorMessageRunFacet = ExternalQueryRunFacet = (
            Error
        ) = ExtractionErrorRunFacet = LifecycleStateChange = LifecycleStateChangeDatasetFacet = (
            PreviousIdentifier
        ) = OutputStatisticsOutputDatasetFacet = SchemaDatasetFacet = SchemaDatasetFacetFields = (
            SQLJobFacet
        ) = Identifier = SymlinksDatasetFacet = create_no_op

__all__ = [
    "BaseFacet",
    "Dataset",
    "DatasetFacet",
    "InputDataset",
    "OutputDataset",
    "RunFacet",
    "ColumnLineageDatasetFacet",
    "Fields",
    "InputField",
    "DocumentationDatasetFacet",
    "ErrorMessageRunFacet",
    "ExternalQueryRunFacet",
    "Error",
    "ExtractionErrorRunFacet",
    "LifecycleStateChange",
    "LifecycleStateChangeDatasetFacet",
    "PreviousIdentifier",
    "OutputStatisticsOutputDatasetFacet",
    "SchemaDatasetFacet",
    "SchemaDatasetFacetFields",
    "SQLJobFacet",
    "Identifier",
    "SymlinksDatasetFacet",
]
