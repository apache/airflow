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

import logging

from airflow.providers.openlineage.extractors.base import BaseExtractor, OperatorLineage
from openlineage.client.run import Dataset

log = logging.getLogger(__name__)

"""
:meta private:
"""


class GCSToGCSExtractor(BaseExtractor):
    """GCSToGCSOperator extractor

    :meta private:
    """

    @classmethod
    def get_operator_classnames(cls) -> list[str]:
        return ["GCSToGCSOperator"]

    def extract(self) -> OperatorLineage | None:
        if self.operator.source_object:
            input_objects = [
                Dataset(
                    namespace=f"gs://{self.operator.source_bucket}",
                    name=f"gs://{self.operator.source_bucket}/{self.operator.source_object}",
                    facets={},
                )
            ]
        else:
            input_objects = [
                Dataset(
                    namespace=f"gs://{self.operator.source_bucket}",
                    name=f"gs://{self.operator.source_bucket}/{source_object}",
                    facets={},
                )
                for source_object in self.operator.source_objects
            ]

        output_object = Dataset(
            namespace=f"gs://{self.operator.destination_bucket}",
            name=f"gs://{self.operator.destination_bucket}/{self.operator.destination_object}",
            facets={},
        )

        return OperatorLineage(
            inputs=input_objects,
            outputs=[output_object],
        )
