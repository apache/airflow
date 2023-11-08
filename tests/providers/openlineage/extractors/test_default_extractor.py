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

from typing import Any
from unittest import mock

import pytest
from attrs import Factory, define, field
from openlineage.client.facet import BaseFacet, ParentRunFacet, SqlJobFacet
from openlineage.client.run import Dataset

from airflow.models.baseoperator import BaseOperator
from airflow.operators.python import PythonOperator
from airflow.providers.openlineage.extractors.base import (
    DefaultExtractor,
    OperatorLineage,
)
from airflow.providers.openlineage.extractors.manager import ExtractorManager
from airflow.providers.openlineage.extractors.python import PythonExtractor

pytestmark = pytest.mark.db_test


INPUTS = [Dataset(namespace="database://host:port", name="inputtable")]
OUTPUTS = [Dataset(namespace="database://host:port", name="inputtable")]
RUN_FACETS: dict[str, BaseFacet] = {
    "parent": ParentRunFacet.create("3bb703d1-09c1-4a42-8da5-35a0b3216072", "namespace", "parentjob")
}
JOB_FACETS: dict[str, BaseFacet] = {"sql": SqlJobFacet(query="SELECT * FROM inputtable")}


@define
class CompleteRunFacet(BaseFacet):
    finished: bool = field(default=False)


FINISHED_FACETS: dict[str, BaseFacet] = {"complete": CompleteRunFacet(True)}


class ExampleOperator(BaseOperator):
    def execute(self, context) -> Any:
        pass

    def get_openlineage_facets_on_start(self) -> OperatorLineage:
        return OperatorLineage(
            inputs=INPUTS,
            outputs=OUTPUTS,
            run_facets=RUN_FACETS,
            job_facets=JOB_FACETS,
        )

    def get_openlineage_facets_on_complete(self, task_instance) -> OperatorLineage:
        return OperatorLineage(
            inputs=INPUTS,
            outputs=OUTPUTS,
            run_facets=RUN_FACETS,
            job_facets=FINISHED_FACETS,
        )


class OperatorWithoutComplete(BaseOperator):
    def execute(self, context) -> Any:
        pass

    def get_openlineage_facets_on_start(self) -> OperatorLineage:
        return OperatorLineage(
            inputs=INPUTS,
            outputs=OUTPUTS,
            run_facets=RUN_FACETS,
            job_facets=JOB_FACETS,
        )


class OperatorWithoutStart(BaseOperator):
    def execute(self, context) -> Any:
        pass

    def get_openlineage_facets_on_complete(self, task_instance) -> OperatorLineage:
        return OperatorLineage(
            inputs=INPUTS,
            outputs=OUTPUTS,
            run_facets=RUN_FACETS,
            job_facets=FINISHED_FACETS,
        )


class OperatorDifferentOperatorLineageClass(BaseOperator):
    def execute(self, context) -> Any:
        pass

    def get_openlineage_facets_on_start(self):
        @define
        class DifferentOperatorLineage:
            name: str = ""
            inputs: list[Dataset] = Factory(list)
            outputs: list[Dataset] = Factory(list)
            run_facets: dict[str, BaseFacet] = Factory(dict)
            job_facets: dict[str, BaseFacet] = Factory(dict)
            some_other_param: dict = Factory(dict)

        return DifferentOperatorLineage(  # type: ignore
            name="unused",
            inputs=INPUTS,
            outputs=OUTPUTS,
            run_facets=RUN_FACETS,
            job_facets=JOB_FACETS,
            some_other_param={"asdf": "fdsa"},
        )


class OperatorWrongOperatorLineageClass(BaseOperator):
    def execute(self, context) -> Any:
        pass

    def get_openlineage_facets_on_start(self):
        @define
        class WrongOperatorLineage:
            inputs: list[Dataset] = Factory(list)
            outputs: list[Dataset] = Factory(list)
            some_other_param: dict = Factory(dict)

        return WrongOperatorLineage(  # type: ignore
            inputs=INPUTS,
            outputs=OUTPUTS,
            some_other_param={"asdf": "fdsa"},
        )


class BrokenOperator(BaseOperator):
    get_openlineage_facets: list[BaseFacet] = []

    def execute(self, context) -> Any:
        pass


def test_default_extraction():
    extractor = ExtractorManager().get_extractor_class(ExampleOperator)
    assert extractor is DefaultExtractor

    metadata = extractor(ExampleOperator(task_id="test")).extract()

    task_instance = mock.MagicMock()

    metadata_on_complete = extractor(ExampleOperator(task_id="test")).extract_on_complete(
        task_instance=task_instance
    )

    assert metadata == OperatorLineage(
        inputs=INPUTS,
        outputs=OUTPUTS,
        run_facets=RUN_FACETS,
        job_facets=JOB_FACETS,
    )

    assert metadata_on_complete == OperatorLineage(
        inputs=INPUTS,
        outputs=OUTPUTS,
        run_facets=RUN_FACETS,
        job_facets=FINISHED_FACETS,
    )


def test_extraction_without_on_complete():
    extractor = ExtractorManager().get_extractor_class(OperatorWithoutComplete)
    assert extractor is DefaultExtractor

    metadata = extractor(OperatorWithoutComplete(task_id="test")).extract()

    task_instance = mock.MagicMock()

    metadata_on_complete = extractor(OperatorWithoutComplete(task_id="test")).extract_on_complete(
        task_instance=task_instance
    )

    expected_task_metadata = OperatorLineage(
        inputs=INPUTS,
        outputs=OUTPUTS,
        run_facets=RUN_FACETS,
        job_facets=JOB_FACETS,
    )

    assert metadata == expected_task_metadata

    assert metadata_on_complete == expected_task_metadata


def test_extraction_without_on_start():
    extractor = ExtractorManager().get_extractor_class(OperatorWithoutStart)
    assert extractor is DefaultExtractor

    metadata = extractor(OperatorWithoutStart(task_id="test")).extract()

    task_instance = mock.MagicMock()

    metadata_on_complete = extractor(OperatorWithoutStart(task_id="test")).extract_on_complete(
        task_instance=task_instance
    )

    assert metadata is None

    assert metadata_on_complete == OperatorLineage(
        inputs=INPUTS,
        outputs=OUTPUTS,
        run_facets=RUN_FACETS,
        job_facets=FINISHED_FACETS,
    )


def test_does_not_use_default_extractor_when_not_a_method():
    extractor_class = ExtractorManager().get_extractor_class(BrokenOperator(task_id="a"))
    assert extractor_class is None


def test_does_not_use_default_extractor_when_no_get_openlineage_facets():
    extractor_class = ExtractorManager().get_extractor_class(BaseOperator(task_id="b"))
    assert extractor_class is None


def test_does_not_use_default_extractor_when_explicite_extractor():
    extractor_class = ExtractorManager().get_extractor_class(
        PythonOperator(task_id="c", python_callable=lambda: 7)
    )
    assert extractor_class is PythonExtractor


def test_default_extractor_uses_different_operatorlineage_class():
    operator = OperatorDifferentOperatorLineageClass(task_id="task_id")
    extractor_class = ExtractorManager().get_extractor_class(operator)
    assert extractor_class is DefaultExtractor
    extractor = extractor_class(operator)
    assert extractor.extract() == OperatorLineage(
        inputs=INPUTS,
        outputs=OUTPUTS,
        run_facets=RUN_FACETS,
        job_facets=JOB_FACETS,
    )


def test_default_extractor_uses_wrong_operatorlineage_class():
    operator = OperatorWrongOperatorLineageClass(task_id="task_id")
    # If extractor returns lineage class that can't be changed into OperatorLineage, just return
    # empty OperatorLineage
    assert (
        ExtractorManager().extract_metadata(mock.MagicMock(), operator, complete=False) == OperatorLineage()
    )
