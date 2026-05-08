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
        super().__init__()
        self.edc_hook = edc_hook

    def get_object(self, object_id: str) -> dict[str, Any]:
        """Return Informatica catalog object by id via EDC hook."""
        return self.edc_hook.get_object(object_id)

    def create_lineage_link(self, source_object_id: str, target_object_id: str) -> dict[str, Any]:
        """Create a lineage link between source and target objects via EDC hook."""
        return self.edc_hook.create_lineage_link(source_object_id, target_object_id)

    def find_object_id(
        self,
        catalog_name: str,
        database_name: str,
        table_name: str,
    ) -> str | None:
        """
        Find Informatica catalog object id by catalog, database, and table name.

        Resolves the EDC object identifier by searching for the table by name,
        then validating it belongs to the expected schema/database hierarchy.

        :param catalog_name: Name of the catalog (used to disambiguate schemas).
        :param database_name: Name of the database / schema.
        :param table_name: Name of the table or view.
        :return: Informatica catalog object id if found, else None.
        """
        table_obj = None

        tables = self.edc_hook.search_table(table_name)
        if not tables.get("hits"):
            self.log.debug("No EDC objects found for table %r", table_name)
            return None

        for table in tables["hits"]:
            if len(tables["hits"]) == 1:
                table_obj = self.edc_hook.get_object(table["id"])
                break

            schemas = self.edc_hook.search_schema(database_name)
            for schema in schemas.get("hits", []):
                if not str(table["id"]).startswith(schema["id"]):
                    continue

                if len(schemas["hits"]) == 1:
                    table_obj = self.edc_hook.get_object(table["id"])
                    break

                databases = self.edc_hook.search_database(catalog_name)
                for database in databases.get("hits", []):
                    if str(schema["id"]).startswith(database["id"]):
                        table_obj = self.edc_hook.get_object(table["id"])
                        break

                if table_obj:
                    break

            if table_obj:
                break

        if table_obj:
            return table_obj["id"]

        self.log.debug(
            "Could not resolve EDC object for catalog=%r, database=%r, table=%r",
            catalog_name,
            database_name,
            table_name,
        )
        return None
