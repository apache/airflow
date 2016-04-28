# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Imports the hooks dynamically while keeping the package API clean,
# abstracting the underlying modules
from airflow.utils.helpers import import_module_attrs as _import_module_attrs

_hooks = {
    'ftp_hook': ['FTPHook'],
    'ftps_hook': ['FTPSHook'],
    'vertica_hook': ['VerticaHook'],
    'ssh_hook': ['SSHHook'],
    'bigquery_hook': ['BigQueryHook'],
    'qubole_hook': ['QuboleHook'],
    'gcs_hook': ['GoogleCloudStorageHook'],
    'datastore_hook': ['DatastoreHook'],
    'cloudant_hook': ['CloudantHook']
}

_import_module_attrs(globals(), _hooks)
