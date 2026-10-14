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

from airflow.providers.google.cloud.links.datastore import (
    CloudDatastoreEntitiesLink,
    CloudDatastoreImportExportLink,
)

EXPECTED_CLOUD_DATASTORE_IMPORT_EXPORT_LINK_NAME = "Import/Export Page"
EXPECTED_CLOUD_DATASTORE_IMPORT_EXPORT_LINK_KEY = "import_export_conf"
EXPECTED_CLOUD_DATASTORE_IMPORT_EXPORT_LINK_FORMAT_STR = "/datastore/import-export?project={project_id}"
EXPECTED_CLOUD_DATASTORE_ENTITIES_LINK_NAME = "Entities"
EXPECTED_CLOUD_DATASTORE_ENTITIES_LINK_KEY = "entities_conf"
EXPECTED_CLOUD_DATASTORE_ENTITIES_LINK_FORMAT_STR = "/datastore/entities/query/kind?project={project_id}"


class TestCloudDatastoreImportExportLink:
    def test_class_attributes(self):
        assert CloudDatastoreImportExportLink.name == EXPECTED_CLOUD_DATASTORE_IMPORT_EXPORT_LINK_NAME
        assert CloudDatastoreImportExportLink.key == EXPECTED_CLOUD_DATASTORE_IMPORT_EXPORT_LINK_KEY
        assert (
            CloudDatastoreImportExportLink.format_str
            == EXPECTED_CLOUD_DATASTORE_IMPORT_EXPORT_LINK_FORMAT_STR
        )


class TestCloudDatastoreEntitiesLink:
    def test_class_attributes(self):
        assert CloudDatastoreEntitiesLink.name == EXPECTED_CLOUD_DATASTORE_ENTITIES_LINK_NAME
        assert CloudDatastoreEntitiesLink.key == EXPECTED_CLOUD_DATASTORE_ENTITIES_LINK_KEY
        assert CloudDatastoreEntitiesLink.format_str == EXPECTED_CLOUD_DATASTORE_ENTITIES_LINK_FORMAT_STR
