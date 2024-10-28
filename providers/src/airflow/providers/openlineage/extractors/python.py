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

import inspect
from typing import Callable

from openlineage.client.facet_v2 import source_code_job

from airflow.providers.openlineage import conf
from airflow.providers.openlineage.extractors.base import BaseExtractor, OperatorLineage
from airflow.providers.openlineage.utils.utils import (
    get_unknown_source_attribute_run_facet,
)

"""
:meta private:
"""


class PythonExtractor(BaseExtractor):
    """
    Extract executed source code and put it into SourceCodeJobFacet.

    This extractor provides visibility on what particular task does by extracting
    executed source code and putting it into SourceCodeJobFacet. It does not extract
    datasets yet.

    :meta private:
    """

    @classmethod
    def get_operator_classnames(cls) -> list[str]:
        return ["PythonOperator"]

    def _execute_extraction(self) -> OperatorLineage | None:
        source_code = self.get_source_code(self.operator.python_callable)
        job_facet: dict = {}
        if conf.is_source_enabled() and source_code:
            job_facet = {
                "sourceCode": source_code_job.SourceCodeJobFacet(
                    language="python",
                    # We're on worker and should have access to DAG files
                    sourceCode=source_code,
                )
            }
        else:
            self.log.debug(
                "OpenLineage disable_source_code option is on - no source code is extracted.",
            )

        return OperatorLineage(
            job_facets=job_facet,
            # The PythonOperator is recorded as an "unknownSource" even though we have an extractor,
            # as the <i>data lineage</i> cannot be determined from the operator directly.
            run_facets=get_unknown_source_attribute_run_facet(
                task=self.operator, name="PythonOperator"
            ),
        )

    def get_source_code(self, callable: Callable) -> str | None:
        try:
            return inspect.getsource(callable)
        except TypeError:
            # Trying to extract source code of builtin_function_or_method
            return str(callable)
        except OSError:
            self.log.warning(
                "Can't get source code facet of PythonOperator %s",
                self.operator.task_id,
            )
        return None

    def extract(self) -> OperatorLineage | None:
        return super().extract()
