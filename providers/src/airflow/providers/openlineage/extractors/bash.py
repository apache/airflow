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

from openlineage.client.facet_v2 import source_code_job

from airflow.providers.openlineage import conf
from airflow.providers.openlineage.extractors.base import BaseExtractor, OperatorLineage
from airflow.providers.openlineage.utils.utils import get_unknown_source_attribute_run_facet

"""
:meta private:
"""


class BashExtractor(BaseExtractor):
    """
    Extract executed bash command and put it into SourceCodeJobFacet.

    This extractor provides visibility on what bash task does by extracting
    executed bash command and putting it into SourceCodeJobFacet. It does
    not extract datasets.

    :meta private:
    """

    @classmethod
    def get_operator_classnames(cls) -> list[str]:
        return ["BashOperator"]

    def _execute_extraction(self) -> OperatorLineage | None:
        job_facets: dict = {}
        if conf.is_source_enabled():
            job_facets = {
                "sourceCode": source_code_job.SourceCodeJobFacet(
                    language="bash",
                    # We're on worker and should have access to DAG files
                    sourceCode=self.operator.bash_command,
                )
            }
        else:
            self.log.debug(
                "OpenLineage disable_source_code option is on - no source code is extracted.",
            )

        return OperatorLineage(
            job_facets=job_facets,
            # The BashOperator is recorded as an "unknownSource" even though we have an extractor,
            # as the <i>data lineage</i> cannot be determined from the operator directly.
            run_facets=get_unknown_source_attribute_run_facet(task=self.operator, name="BashOperator"),
        )

    def extract(self) -> OperatorLineage | None:
        return super().extract()
