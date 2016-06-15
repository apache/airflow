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



# Contrib hooks are not imported by default. They should be accessed
# directly: from airflow.contrib.hooks.hook_module import Hook







# ------------------------------------------------------------------------
#
# #TODO #FIXME Airflow 2.0
#
# Old import machinary below.
#
# This is deprecated but should be kept until Airflow 2.0
# for compatibility.
#
# ------------------------------------------------------------------------

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
    'gcp_dataproc_hook': ['DataProcHook'],
    'cloudant_hook': ['CloudantHook'],
    'fs_hook': ['FSHook']
}

import os as _os
if not _os.environ.get('AIRFLOW_USE_NEW_IMPORTS', False):
    from zope.deprecation import deprecated as _deprecated
    _imported = _import_module_attrs(globals(), _hooks)
    for _i in _imported:
        _deprecated(
            _i,
            "Importing {i} directly from 'contrib.hooks' has been "
            "deprecated. Please import from "
            "'contrib.hooks.[hook_module]' instead. Support for direct imports "
            "will be dropped entirely in Airflow 2.0.".format(i=_i))
