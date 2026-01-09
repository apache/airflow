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
"""This module contains Google Data Catalog links."""

from __future__ import annotations

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.google.cloud.links.base import BaseGoogleLink
from airflow.providers.google.common.deprecated import deprecated

DATACATALOG_BASE_LINK = "/datacatalog"
ENTRY_GROUP_LINK = (
    DATACATALOG_BASE_LINK
    + "/groups/{entry_group_id};container={project_id};location={location_id}?project={project_id}"
)
ENTRY_LINK = (
    DATACATALOG_BASE_LINK
    + "/projects/{project_id}/locations/{location_id}/entryGroups/{entry_group_id}/entries/{entry_id}\
    ?project={project_id}"
)
TAG_TEMPLATE_LINK = (
    DATACATALOG_BASE_LINK
    + "/projects/{project_id}/locations/{location_id}/tagTemplates/{tag_template_id}?project={project_id}"
)


@deprecated(
    planned_removal_date="January 30, 2026",
    use_instead="airflow.providers.google.cloud.links.dataplex.DataplexCatalogEntryGroupLink",
    reason="The Data Catalog will be discontinued on January 30, 2026 "
    "in favor of Dataplex Universal Catalog.",
    category=AirflowProviderDeprecationWarning,
)
class DataCatalogEntryGroupLink(BaseGoogleLink):
    """Helper class for constructing Data Catalog Entry Group Link."""

    name = "Data Catalog Entry Group"
    key = "data_catalog_entry_group"
    format_str = ENTRY_GROUP_LINK


@deprecated(
    planned_removal_date="January 30, 2026",
    use_instead="airflow.providers.google.cloud.links.dataplex.DataplexCatalogEntryLink",
    reason="The Data Catalog will be discontinued on January 30, 2026 "
    "in favor of Dataplex Universal Catalog.",
    category=AirflowProviderDeprecationWarning,
)
class DataCatalogEntryLink(BaseGoogleLink):
    """Helper class for constructing Data Catalog Entry Link."""

    name = "Data Catalog Entry"
    key = "data_catalog_entry"
    format_str = ENTRY_LINK


@deprecated(
    planned_removal_date="January 30, 2026",
    use_instead="airflow.providers.google.cloud.links.dataplex.DataplexCatalogAspectTypeLink",
    reason="The Data Catalog will be discontinued on January 30, 2026 "
    "in favor of Dataplex Universal Catalog.",
    category=AirflowProviderDeprecationWarning,
)
class DataCatalogTagTemplateLink(BaseGoogleLink):
    """Helper class for constructing Data Catalog Tag Template Link."""

    name = "Data Catalog Tag Template"
    key = "data_catalog_tag_template"
    format_str = TAG_TEMPLATE_LINK
