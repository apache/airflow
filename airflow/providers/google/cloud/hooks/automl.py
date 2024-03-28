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

import warnings

from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning


class CloudAutoMLHook:
    """
    Former Google Cloud AutoML hook.

    Deprecated as AutoML API becomes unusable starting March 31, 2024:
    https://cloud.google.com/automl/docs
    """

    deprecation_warning = (
        "CloudAutoMLHook has been deprecated, as AutoML API becomes unusable starting "
        "March 31, 2024, and will be removed in future release. Please use an equivalent "
        " Vertex AI hook available in"
        "airflow.providers.google.cloud.hooks.vertex_ai instead."
    )

    method_exception = "This method cannot be used as AutoML API becomes unusable."

    def __init__(self, **_) -> None:
        warnings.warn(self.deprecation_warning, AirflowProviderDeprecationWarning)

    @staticmethod
    def extract_object_id(obj: dict) -> str:
        """Return unique id of the object."""
        warnings.warn(
            "'extract_object_id' method is deprecated and will be removed in future release.",
            AirflowProviderDeprecationWarning,
        )
        return obj["name"].rpartition("/")[-1]

    def get_conn(self):
        """
        Retrieve connection to AutoML (deprecated).

        :raises: AirflowException
        """
        raise AirflowException(self.method_exception)

    def wait_for_operation(self, **_):
        """
        Wait for long-lasting operation to complete (deprecated).

        :raises: AirflowException
        """
        raise AirflowException(self.method_exception)

    def prediction_client(self, **_):
        """
        Create a PredictionServiceClient (deprecated).

        :raises: AirflowException
        """
        raise AirflowException(self.method_exception)

    def create_model(self, **_):
        """
        Create a model_id and returns a Model in the `response` field when it completes (deprecated).

        :raises: AirflowException
        """
        raise AirflowException(self.method_exception)

    def batch_predict(self, **_):
        """
        Perform a batch prediction (deprecated).

        :raises: AirflowException
        """
        raise AirflowException(self.method_exception)

    def predict(self, **_):
        """
        Perform an online prediction (deprecated).

        :raises: AirflowException
        """
        raise AirflowException(self.method_exception)

    def create_dataset(self, **_):
        """
        Create a dataset (deprecated).

        :raises: AirflowException
        """
        raise AirflowException(self.method_exception)

    def import_data(self, **_):
        """
        Import data (deprecated).

        :raises: AirflowException
        """
        raise AirflowException(self.method_exception)

    def list_column_specs(self, **_):
        """
        List column specs (deprecated).

        :raises: AirflowException
        """
        raise AirflowException(self.method_exception)

    def get_model(self, **_):
        """
        Get a model (deprecated).

        :raises: AirflowException
        """
        raise AirflowException(self.method_exception)

    def delete_model(self, **_):
        """
        Delete a model (deprecated).

        :raises: AirflowException
        """
        raise AirflowException(self.method_exception)

    def update_dataset(self, **_):
        """
        Update a model (deprecated).

        :raises: AirflowException
        """
        raise AirflowException(self.method_exception)

    def deploy_model(self, **_):
        """
        Deploy a model (deprecated).

        :raises: AirflowException
        """
        raise AirflowException(self.method_exception)

    def list_table_specs(self, **_):
        """
        List table specs (deprecated).

        :raises: AirflowException
        """
        raise AirflowException(self.method_exception)

    def list_datasets(self, **_):
        """
        List datasets (deprecated).

        :raises: AirflowException
        """
        raise AirflowException(self.method_exception)

    def delete_dataset(self, **_):
        """
        Delete a dataset (deprecated).

        :raises: AirflowException
        """
        raise AirflowException(self.method_exception)
