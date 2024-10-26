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

from typing import TYPE_CHECKING, Sequence

from google.api_core.client_options import ClientOptions
from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.cloud.aiplatform_v1 import PredictionServiceClient

from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID, GoogleBaseHook

if TYPE_CHECKING:
    from google.api_core.retry import Retry
    from google.cloud.aiplatform_v1.types import PredictResponse


class PredictionServiceHook(GoogleBaseHook):
    """Hook for Google Cloud Vertex AI Prediction API."""

    def get_prediction_service_client(self, region: str | None = None) -> PredictionServiceClient:
        """
        Return PredictionServiceClient object.

        :param region: The ID of the Google Cloud region that the service belongs to. Default is None.

        :return: `google.cloud.aiplatform_v1.services.prediction_service.client.PredictionServiceClient` instance.
        """
        if region and region != "global":
            client_options = ClientOptions(api_endpoint=f"{region}-aiplatform.googleapis.com:443")
        else:
            client_options = ClientOptions()

        return PredictionServiceClient(
            credentials=self.get_credentials(), client_info=CLIENT_INFO, client_options=client_options
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def predict(
        self,
        endpoint_id: str,
        instances: list[str],
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        parameters: dict[str, str] | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> PredictResponse:
        """
        Perform an online prediction and returns the prediction result in the response.

        :param endpoint_id: Name of the endpoint_id requested to serve the prediction.
        :param instances: Required. The instances that are the input to the prediction call. A DeployedModel
            may have an upper limit on the number of instances it supports per request, and when it is
            exceeded the prediction call errors in case of AutoML Models, or, in case of customer created
            Models, the behaviour is as documented by that Model.
        :param parameters: Additional domain-specific parameters, any string must be up to 25000 characters long.
        :param project_id: ID of the Google Cloud project where model is located if None then
            default project_id is used.
        :param location: The location of the project.
        :param retry: A retry object used to retry requests. If `None` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        client = self.get_prediction_service_client(location)
        endpoint = f"projects/{project_id}/locations/{location}/endpoints/{endpoint_id}"
        return client.predict(
            request={"endpoint": endpoint, "instances": instances, "parameters": parameters},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
