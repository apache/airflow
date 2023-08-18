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

from abc import ABC, abstractmethod

from attrs import Factory, define
from openlineage.client.facet import BaseFacet
from openlineage.client.run import Dataset

from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import TaskInstanceState


@define
class OperatorLineage:
    """Structure returned from lineage extraction."""

    inputs: list[Dataset] = Factory(list)
    outputs: list[Dataset] = Factory(list)
    run_facets: dict[str, BaseFacet] = Factory(dict)
    job_facets: dict[str, BaseFacet] = Factory(dict)


class BaseExtractor(ABC, LoggingMixin):
    """Abstract base extractor class.

    This is used mostly to maintain support for custom extractors.
    """

    _allowed_query_params: list[str] = []

    def __init__(self, operator):  # type: ignore
        super().__init__()
        self.operator = operator

    @classmethod
    @abstractmethod
    def get_operator_classnames(cls) -> list[str]:
        """Get a list of operators that extractor works for.

        This is an abstract method that subclasses should implement. There are
        operators that work very similarly and one extractor can cover.
        """
        raise NotImplementedError()

    def validate(self):
        assert self.operator.task_type in self.get_operator_classnames()

    @abstractmethod
    def extract(self) -> OperatorLineage | None:
        pass

    def extract_on_complete(self, task_instance) -> OperatorLineage | None:
        return self.extract()


class DefaultExtractor(BaseExtractor):
    """Extractor that uses `get_openlineage_facets_on_start/complete/failure` methods."""

    @classmethod
    def get_operator_classnames(cls) -> list[str]:
        """Assign this extractor to *no* operators.

        Default extractor is chosen not on the classname basis, but
        by existence of get_openlineage_facets method on operator.
        """
        return []

    def extract(self) -> OperatorLineage | None:
        # OpenLineage methods are optional - if there's no method, return None
        try:
            return self._get_openlineage_facets(self.operator.get_openlineage_facets_on_start)  # type: ignore
        except ImportError:
            self.log.error(
                "OpenLineage provider method failed to import OpenLineage integration. "
                "This should not happen. Please report this bug to developers."
            )
            return None
        except AttributeError:
            return None

    def extract_on_complete(self, task_instance) -> OperatorLineage | None:
        if task_instance.state == TaskInstanceState.FAILED:
            on_failed = getattr(self.operator, "get_openlineage_facets_on_failure", None)
            if on_failed and callable(on_failed):
                return self._get_openlineage_facets(on_failed, task_instance)
        on_complete = getattr(self.operator, "get_openlineage_facets_on_complete", None)
        if on_complete and callable(on_complete):
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
            self.log.exception("OpenLineage provider method failed to extract data from provider. ")
        return None
