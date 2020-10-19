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
"""
This module contains the Great Expectations Hook for BigQuery
"""
import datetime

from airflow.providers.greatexpectations.hooks.great_expectations import GreatExpectationsHook
from airflow.providers.greatexpectations.hooks.great_expectations import GreatExpectationsValidations

from great_expectations.data_context.types.base import DataContextConfig
from airflow.providers.greatexpectations.hooks.great_expectations import GreatExpectationsConnection


class GreatExpectationsBigQueryGCSHook(GreatExpectationsHook):

    def __init__(self, gcp_project, bq_dataset_name, credentials_path, validation_type, validation_type_input,
                 gcs_bucket, gcs_expectations_prefix, gcs_validations_prefix, gcs_datadocs_prefix, **kwargs):
        self.gcp_project = gcp_project
        self.bq_dataset_name = bq_dataset_name
        self.credentials_path = credentials_path
        self.validation_type = validation_type
        self.validation_type_input = validation_type_input
        self.gcs_bucket = gcs_bucket
        self.gcs_expectations_prefix = gcs_expectations_prefix
        self.gcs_validations_prefix = gcs_validations_prefix
        self.gcs_datadocs_prefix = gcs_datadocs_prefix

        super().__init__(**kwargs)

    def get_conn(self):
        data_context_config = self.create_data_context_config()
        return GreatExpectationsConnection(data_context_config)

    def create_data_context_config(self):
        data_context_config = DataContextConfig(
            config_version=2,
            datasources={
                "bq_datasource": {
                    "credentials": {
                        "url": "bigquery://" + self.gcp_project + "/" + self.bq_dataset_name + "?credentials_path=" +
                               self.credentials_path
                    },
                    "class_name": "SqlAlchemyDatasource",
                    "module_name": "great_expectations.datasource",
                    "data_asset_type": {
                        "module_name": "great_expectations.dataset",
                        "class_name": "SqlAlchemyDataset"
                    }
                }
            },
            expectations_store_name="expectations_GCS_store",
            validations_store_name="validations_GCS_store",
            evaluation_parameter_store_name="evaluation_parameter_store",
            plugins_directory=None,
            validation_operators={
                "action_list_operator": {
                    "class_name": "ActionListValidationOperator",
                    "action_list": [
                        {
                            "name": "store_validation_result",
                            "action": {"class_name": "StoreValidationResultAction"},
                        },
                        {
                            "name": "store_evaluation_params",
                            "action": {"class_name": "StoreEvaluationParametersAction"},
                        },
                        {
                            "name": "update_data_docs",
                            "action": {"class_name": "UpdateDataDocsAction"},
                        },
                    ],
                }
            },
            stores={
                'expectations_GCS_store': {
                    'class_name': 'ExpectationsStore',
                    'store_backend': {
                        'class_name': 'TupleGCSStoreBackend',
                        'project': self.gcp_project,
                        'bucket': self.gcs_bucket,
                        'prefix': self.gcs_expectations_prefix
                    }
                },
                'validations_GCS_store': {
                    'class_name': 'ValidationsStore',
                    'store_backend': {
                        'class_name': 'TupleGCSStoreBackend',
                        'project': self.gcp_project,
                        'bucket': self.gcs_bucket,
                        'prefix': self.gcs_validations_prefix
                    }
                },
                "evaluation_parameter_store": {"class_name": "EvaluationParameterStore"},
            },
            data_docs_sites={
                "GCS_site": {
                    "class_name": "SiteBuilder",
                    "store_backend": {
                        "class_name": "TupleGCSStoreBackend",
                        "project": self.gcp_project,
                        "bucket": self.gcs_bucket,
                        'prefix': self.gcs_datadocs_prefix
                    },
                    "site_index_builder": {
                        "class_name": "DefaultSiteIndexBuilder",
                    },
                }
            },
            config_variables_file_path=None,
            commented_map=None,
        )
        return data_context_config

    def get_batch_kwargs(self):
        # Tell GE where to fetch the batch of data to be validated.
        batch_kwargs = {
            "datasource": "bq_datasource",
        }

        if self.validation_type == GreatExpectationsValidations.SQL.value:
            batch_kwargs["query"] = self.validation_type_input
            batch_kwargs["data_asset_name"] = self.bq_dataset_name
            batch_kwargs["bigquery_temp_table"] = self.get_temp_table_name(
                'temp_ge_' + datetime.datetime.now().strftime('%Y%m%d') + '_', 10)
        elif self.validation_type == GreatExpectationsValidations.TABLE.value:
            batch_kwargs["table"] = self.validation_type_input
            batch_kwargs["data_asset_name"] = self.bq_dataset_name

        self.log.info("batch_kwargs: " + str(batch_kwargs))

        return batch_kwargs
