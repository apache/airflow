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

from airflow.providers.openlineage.extractors.base import BaseExtractor, OperatorLineage
from airflow.providers.openlineage.plugins.facets import (
    UnknownOperatorAttributeRunFacet,
    UnknownOperatorInstance,
)
from airflow.providers.openlineage.utils.utils import get_filtered_unknown_operator_keys, is_source_enabled
from openlineage.client.facet import SourceCodeJobFacet

"""
:meta private:
"""


class PythonExtractor(BaseExtractor):
    """
    This extractor provides visibility on what particular task does by extracting
    executed source code and putting it into SourceCodeJobFacet. It does not extract
    datasets yet.

    :meta private:
    """

    @classmethod
    def get_operator_classnames(cls) -> list[str]:
        return ["PythonOperator"]

    def extract(self) -> OperatorLineage | None:
        source_code = self.get_source_code(self.operator.python_callable)
        job_facet: dict = {}
        if is_source_enabled() and source_code:
            job_facet = {
                "sourceCode": SourceCodeJobFacet(
                    language="python",
                    # We're on worker and should have access to DAG files
                    source=source_code,
                )
            }
        return OperatorLineage(
            job_facets=job_facet,
            run_facets={
                # The PythonOperator is recorded as an "unknownSource" even though we have an
                # extractor, as the data lineage cannot be determined from the operator
                # directly.
                "unknownSourceAttribute": UnknownOperatorAttributeRunFacet(
                    unknownItems=[
                        UnknownOperatorInstance(
                            name="PythonOperator",
                            properties=get_filtered_unknown_operator_keys(self.operator),
                        )
                    ]
                )
            },
        )

    def get_source_code(self, callable: Callable) -> str | None:
        try:
            return inspect.getsource(callable)
        except TypeError:
            # Trying to extract source code of builtin_function_or_method
            return str(callable)
        except OSError:
            self.log.exception("Can't get source code facet of PythonOperator %s", self.operator.task_id)
        return None
