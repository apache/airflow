# -*- coding: utf-8 -*-
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
#


# Contrib hooks are not imported by default. They should be accessed
# directly: from airflow.contrib.hooks.hook_module import Hook


import sys
import os as _os

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
_hooks = {
    'docker_hook': ['DockerHook'],
    'ftp_hook': ['FTPHook'],
    'ftps_hook': ['FTPSHook'],
    'vertica_hook': ['VerticaHook'],
    'ssh_hook': ['SSHHook'],
    'winrm_hook': ['WinRMHook'],
    'sftp_hook': ['SFTPHook'],
    'bigquery_hook': ['BigQueryHook'],
    'qubole_hook': ['QuboleHook'],
    'gcs_hook': ['GoogleCloudStorageHook'],
    'datastore_hook': ['DatastoreHook'],
    'gcp_cloudml_hook': ['CloudMLHook'],
    'redshift_hook': ['RedshiftHook'],
    'gcp_dataproc_hook': ['DataProcHook'],
    'gcp_dataflow_hook': ['DataFlowHook'],
    'spark_submit_operator': ['SparkSubmitOperator'],
    'cloudant_hook': ['CloudantHook'],
    'fs_hook': ['FSHook'],
    'wasb_hook': ['WasbHook'],
    'gcp_pubsub_hook': ['PubSubHook'],
    'jenkins_hook': ['JenkinsHook'],
    'aws_dynamodb_hook': ['AwsDynamoDBHook'],
    'azure_data_lake_hook': ['AzureDataLakeHook'],
    'azure_fileshare_hook': ['AzureFileShareHook'],
}


if not _os.environ.get('AIRFLOW_USE_NEW_IMPORTS', False):
    from airflow.utils.helpers import AirflowImporter

    airflow_importer = AirflowImporter(sys.modules[__name__], _hooks)
