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


# Contrib operators are not imported by default. They should be accessed
# directly: from airflow.contrib.operators.operator_module import Operator


import sys


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
_operators = {
    'ssh_execute_operator': ['SSHExecuteOperator'],
    'vertica_operator': ['VerticaOperator'],
    'vertica_to_hive': ['VerticaToHiveTransfer'],
    'qubole_operator': ['QuboleOperator'],
    'fs_operator': ['FileSensor']
}

import os as _os
if not _os.environ.get('AIRFLOW_USE_NEW_IMPORTS', False):
    from airflow.utils.helpers import AirflowImporter
    airflow_importer = AirflowImporter(sys.modules[__name__], _operators)
