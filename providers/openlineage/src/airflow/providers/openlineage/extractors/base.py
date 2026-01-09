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

import warnings
from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from attrs import Factory, define
from openlineage.client.event_v2 import Dataset as OLDataset

with warnings.catch_warnings():
    warnings.simplefilter("ignore", DeprecationWarning)
    from openlineage.client.facet import BaseFacet as BaseFacet_V1
from openlineage.client.facet_v2 import JobFacet, RunFacet

from airflow.utils.log.logging_mixin import LoggingMixin

# this is not to break static checks compatibility with v1 OpenLineage facet classes
DatasetSubclass = TypeVar("DatasetSubclass", bound=OLDataset)
BaseFacetSubclass = TypeVar("BaseFacetSubclass", bound=BaseFacet_V1 | RunFacet | JobFacet)

OL_METHOD_NAME_START = "get_openlineage_facets_on_start"
OL_METHOD_NAME_COMPLETE = "get_openlineage_facets_on_complete"
OL_METHOD_NAME_FAIL = "get_openlineage_facets_on_failure"


@define
class OperatorLineage(Generic[DatasetSubclass, BaseFacetSubclass]):
    """Structure returned from lineage extraction."""

    inputs: list[DatasetSubclass] = Factory(list)
    outputs: list[DatasetSubclass] = Factory(list)
    run_facets: dict[str, BaseFacetSubclass] = Factory(dict)
    job_facets: dict[str, BaseFacetSubclass] = Factory(dict)


class BaseExtractor(ABC, LoggingMixin):
    """
    Abstract base extractor class.

    This is used mostly to maintain support for custom extractors.
    """

    _allowed_query_params: list[str] = []

    def __init__(self, operator):
        super().__init__()
        self.operator = operator

    @classmethod
    @abstractmethod
    def get_operator_classnames(cls) -> list[str]:
        """
        Get a list of operators that extractor works for.

        This is an abstract method that subclasses should implement. There are
        operators that work very similarly and one extractor can cover.
        """
        raise NotImplementedError()

    @abstractmethod
    def _execute_extraction(self) -> OperatorLineage | None: ...

    def extract(self) -> OperatorLineage | None:
        return self._execute_extraction()

    def extract_on_complete(self, task_instance) -> OperatorLineage | None:
        return self.extract()

    def extract_on_failure(self, task_instance) -> OperatorLineage | None:
        return self.extract_on_complete(task_instance)


class DefaultExtractor(BaseExtractor):
    """Extractor that uses `get_openlineage_facets_on_start/complete/failure` methods."""

    @classmethod
    def get_operator_classnames(cls) -> list[str]:
        """
        Assign this extractor to *no* operators.

        Default extractor is chosen not on the classname basis, but
        by existence of get_openlineage_facets method on operator.
        """
        return []

    def _execute_extraction(self) -> OperatorLineage | None:
        method = getattr(self.operator, OL_METHOD_NAME_START, None)
        if callable(method):
            self.log.debug(
                "Trying to execute '%s' method of '%s'.", OL_METHOD_NAME_START, self.operator.task_type
            )
            return self._get_openlineage_facets(method)
        self.log.debug(
            "Operator '%s' does not have '%s' method.", self.operator.task_type, OL_METHOD_NAME_START
        )
        return OperatorLineage()

    def extract_on_complete(self, task_instance) -> OperatorLineage | None:
        method = getattr(self.operator, OL_METHOD_NAME_COMPLETE, None)
        if callable(method):
            self.log.debug(
                "Trying to execute '%s' method of '%s'.", OL_METHOD_NAME_COMPLETE, self.operator.task_type
            )
            return self._get_openlineage_facets(method, task_instance)
        self.log.debug(
            "Operator '%s' does not have '%s' method.", self.operator.task_type, OL_METHOD_NAME_COMPLETE
        )
        return self.extract()

    def extract_on_failure(self, task_instance) -> OperatorLineage | None:
        method = getattr(self.operator, OL_METHOD_NAME_FAIL, None)
        if callable(method):
            self.log.debug(
                "Trying to execute '%s' method of '%s'.", OL_METHOD_NAME_FAIL, self.operator.task_type
            )
            return self._get_openlineage_facets(method, task_instance)
        self.log.debug(
            "Operator '%s' does not have '%s' method.", self.operator.task_type, OL_METHOD_NAME_FAIL
        )
        return self.extract_on_complete(task_instance)

    def _get_openlineage_facets(self, get_facets_method, *args) -> OperatorLineage | None:
        try:
            facets: OperatorLineage | None = get_facets_method(*args)
            if facets is None:
                self.log.debug("OpenLineage method returned `None`")
                return None
            # "rewrite" OperatorLineage to safeguard against different version of the same class
            # that was existing in openlineage-airflow package outside of Airflow repo
            return OperatorLineage(
                inputs=facets.inputs,
                outputs=facets.outputs,
                run_facets=facets.run_facets,
                job_facets=facets.job_facets,
            )
        except ImportError:
            self.log.exception(
                "OpenLineage provider method failed to import OpenLineage integration. "
                "This should not happen."
            )
        except Exception as e:
            self.log.warning(
                "OpenLineage method failed to extract data from Operator with the following exception: `%s`",
                e,
            )
            self.log.debug("OpenLineage extraction failure details:", exc_info=True)
        return None
