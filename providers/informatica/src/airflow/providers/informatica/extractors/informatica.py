#
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

from typing import TYPE_CHECKING, Any

from airflow.utils.log.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from airflow.providers.informatica.hooks.edc import InformaticaEDCHook


class InformaticaLineageExtractor(LoggingMixin):
    """Extracts lineage information from Informatica EDC and converts to Airflow Assets."""

    def __init__(self, edc_hook: InformaticaEDCHook) -> None:
        """
        Initialize InformaticaLineageExtractor.

        Args:
            edc_hook (InformaticaEDCHook): Hook for Informatica EDC API connection.
        """
        super().__init__()
        self.edc_hook = edc_hook

    def get_object(self, object_id: str) -> dict[str, Any]:
        """
        Return Informatica catalog object by id via EDC hook.

        Args:
            object_id (str): Informatica object id.

        Returns:
            dict[str, Any]: Informatica catalog object.
        """
        return self.edc_hook.get_object(object_id)

    def create_lineage_link(self, source_object_id: str, target_object_id: str) -> dict[str, Any]:
        """
        Create a lineage link between source and target objects via EDC hook.

        Args:
            source_object_id (str): Source Informatica object id.
            target_object_id (str): Target Informatica object id.

        Returns:
            dict[str, Any]: Result of lineage link creation.
        """
        return self.edc_hook.create_lineage_link(source_object_id, target_object_id)
