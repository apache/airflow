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
from typing import Generic, TypeVar, Union

from attrs import Factory, define
from openlineage.client.event_v2 import Dataset as OLDataset

with warnings.catch_warnings():
    warnings.simplefilter("ignore", DeprecationWarning)
    from openlineage.client.facet import BaseFacet as BaseFacet_V1
from openlineage.client.facet_v2 import JobFacet, RunFacet

from airflow.providers.openlineage.utils.utils import IS_AIRFLOW_2_10_OR_HIGHER
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import TaskInstanceState

# this is not to break static checks compatibility with v1 OpenLineage facet classes
DatasetSubclass = TypeVar("DatasetSubclass", bound=OLDataset)
BaseFacetSubclass = TypeVar(
    "BaseFacetSubclass", bound=Union[BaseFacet_V1, RunFacet, JobFacet]
)


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

    def __init__(self, operator):  # type: ignore
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
        # OpenLineage methods are optional - if there's no method, return None
        try:
            self.log.debug(
                "Trying to execute `get_openlineage_facets_on_start` for %s.",
                self.operator.task_type,
            )
            return self._get_openlineage_facets(
                self.operator.get_openlineage_facets_on_start
            )  # type: ignore
        except ImportError:
            self.log.error(
                "OpenLineage provider method failed to import OpenLineage integration. "
                "This should not happen. Please report this bug to developers."
            )
            return None
        except AttributeError:
            self.log.debug(
                "Operator %s does not have the get_openlineage_facets_on_start method.",
                self.operator.task_type,
            )
            return OperatorLineage()

    def extract_on_complete(self, task_instance) -> OperatorLineage | None:
        failed_states = [TaskInstanceState.FAILED, TaskInstanceState.UP_FOR_RETRY]
        if (
            not IS_AIRFLOW_2_10_OR_HIGHER
        ):  # todo: remove when min airflow version >= 2.10.0
            # Before fix (#41053) implemented in Airflow 2.10 TaskInstance's state was still RUNNING when
            # being passed to listener's on_failure method. Since `extract_on_complete()` is only called
            # after task completion, RUNNING state means that we are dealing with FAILED task in < 2.10
            failed_states = [TaskInstanceState.RUNNING]

        if task_instance.state in failed_states:
            on_failed = getattr(self.operator, "get_openlineage_facets_on_failure", None)
            if on_failed and callable(on_failed):
                self.log.debug(
                    "Executing `get_openlineage_facets_on_failure` for %s.",
                    self.operator.task_type,
                )
                return self._get_openlineage_facets(on_failed, task_instance)
        on_complete = getattr(self.operator, "get_openlineage_facets_on_complete", None)
        if on_complete and callable(on_complete):
            self.log.debug(
                "Executing `get_openlineage_facets_on_complete` for %s.",
                self.operator.task_type,
            )
            return self._get_openlineage_facets(on_complete, task_instance)
        return self.extract()

    def _get_openlineage_facets(self, get_facets_method, *args) -> OperatorLineage | None:
        try:
            facets: OperatorLineage = get_facets_method(*args)
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
        except Exception:
            self.log.warning(
                "OpenLineage provider method failed to extract data from provider. "
            )
        return None
