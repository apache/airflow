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

from urllib.parse import urlencode

from airflow_google_provider_resource_cleanup.handlers._base import BaseDeleteHandler
from airflow_google_provider_resource_cleanup.helpers import (
    get_aiplatform_client_options,
    get_resource_path,
    run_command_async,
)
from google.cloud import aiplatform

TIMEOUT = 600


def aiplatform_client(project_id: str, location: str):
    aiplatform.init(project=project_id, location=location)
    return aiplatform


async def __delete_via_curl(
    func_name: str,
    asset_description: str,
    resource: dict,
    prefix: str,
    params: dict | None = None,
) -> None:

    try:
        client_options = get_aiplatform_client_options(resource)
        name = get_resource_path(resource)
        api_base = client_options["api_endpoint"]
        q = f"?{urlencode(params)}" if params is not None else ""
        url = f"https://{api_base}/v1/{name}{q}"
        cmd = f"""
            curl -X DELETE \
                -H "Authorization: Bearer $(gcloud auth print-access-token)" \
                "{url}"
        """
        print(f"{prefix} [{func_name}] Long running operation:", name)
        await run_command_async(cmd)
        print(f"{prefix} [{func_name}] DONE:", name)
    except Exception as e:
        print(f"{prefix} Error while deleting the {asset_description.lower()}!", e)


async def __delete_via_client(func_name: str, asset_desc: str, deleter, resource: dict, prefix: str):
    try:
        name = get_resource_path(resource)

        operation = await deleter(name=name)
        print(f"{prefix} [{func_name}] Long running operation:", operation.operation.name)
        result = await operation.result(timeout=TIMEOUT)
        print(f"{prefix} [{func_name}] Response:", result)
    except Exception as e:
        print(f"{prefix} Error while deleting the {asset_desc.lower()}!", e)


async def _delete_model(resource: dict, prefix: str):
    client_options = get_aiplatform_client_options(resource)
    client = aiplatform.gapic.ModelServiceAsyncClient(client_options=client_options)
    await __delete_via_client(
        "_delete_model",
        "model",
        client.delete_model,
        resource,
        prefix,
    )


async def _delete_endpoint(resource: dict, prefix: str):
    client_options = get_aiplatform_client_options(resource)
    client = aiplatform.gapic.EndpointServiceAsyncClient(client_options=client_options)
    await __delete_via_client(
        "_delete_endpoint",
        "endpoint",
        client.delete_endpoint,
        resource,
        prefix,
    )


async def _delete_dataset(resource: dict, prefix: str):
    client_options = get_aiplatform_client_options(resource)
    client = aiplatform.gapic.DatasetServiceAsyncClient(client_options=client_options)
    await __delete_via_client(
        "_delete_dataset",
        "dataset",
        client.delete_dataset,
        resource,
        prefix,
    )


async def _delete_training_pipeline(resource: dict, prefix: str):
    client_options = get_aiplatform_client_options(resource)
    client = aiplatform.gapic.PipelineServiceAsyncClient(client_options=client_options)
    await __delete_via_client(
        "_delete_training_pipeline",
        "training pipeline",
        client.delete_training_pipeline,
        resource,
        prefix,
    )


async def _delete_pipeline_job(resource: dict, prefix: str):
    """
    curl -X DELETE \
     -H "Authorization: Bearer $(gcloud auth print-access-token)" \
     "https://LOCATION-aiplatform.googleapis.com/v1/projects/PROJECT_ID/locations/LOCATION/pipelineJobs/PIPELINE_RUN_ID"
    """
    await __delete_via_curl(
        "_delete_pipeline_job",
        "pipeline job",
        resource,
        prefix,
    )


async def _delete_tuning_job(resource: dict, prefix: str):
    print(
        f"{prefix} [_delete_tuning_job] TuningJob {get_resource_path(resource)} "
        f"skipped because Google Cloud does not support deleting TuningJobs."
        f"See https://cloud.google.com/vertex-ai/generative-ai/docs/models/gemini-supervised-tuning#quota"
    )
    return False


async def _delete_batch_prediction_job(resource: dict, prefix: str):
    client_options = get_aiplatform_client_options(resource)
    client = aiplatform.gapic.JobServiceAsyncClient(client_options=client_options)
    await __delete_via_client(
        "_delete_batch_prediction_job",
        "batch prediction job",
        client.delete_batch_prediction_job,
        resource,
        prefix,
    )


async def _delete_hyperparameter_tuning_job(resource: dict, prefix: str):
    client_options = get_aiplatform_client_options(resource)
    client = aiplatform.gapic.JobServiceAsyncClient(client_options=client_options)
    await __delete_via_client(
        "_delete_hyperparameter_tuning_job",
        "hyperparameter tuning job",
        client.delete_hyperparameter_tuning_job,
        resource,
        prefix,
    )


async def _delete_feature_online_store(resource: dict, prefix: str):
    """
    curl -X DELETE \
     -H "Authorization: Bearer $(gcloud auth print-access-token)" \
     "https://LOCATION_ID-aiplatform.googleapis.com/v1/projects/PROJECT_ID/locations/LOCATION_ID/featureOnlineStores/FEATUREONLINESTORE_NAME?force=BOOLEAN"
    """
    await __delete_via_curl(
        "_delete_feature_online_store", "feature online store", resource, prefix, params={"force": "true"}
    )


async def _delete_cached_content(resource: dict, prefix: str):
    await __delete_via_curl(
        "_delete_cached_content",
        "cached content",
        resource,
        prefix,
    )


async def _delete_tensorboard(resource: dict, prefix: str):
    client_options = get_aiplatform_client_options(resource)
    client = aiplatform.gapic.TensorboardServiceAsyncClient(client_options=client_options)
    await __delete_via_client(
        "_delete_tensorboard",
        "tensorboard",
        client.delete_tensorboard,
        resource,
        prefix,
    )


async def _delete_custom_job(resource: dict, prefix: str):
    client_options = get_aiplatform_client_options(resource)
    client = aiplatform.gapic.JobServiceAsyncClient(client_options=client_options)
    await __delete_via_client(
        "_delete_custom_job",
        "custom job",
        client.delete_custom_job,
        resource,
        prefix,
    )


async def _delete_metadata_store(resource: dict, prefix: str):
    client_options = get_aiplatform_client_options(resource)
    client = aiplatform.gapic.MetadataServiceAsyncClient(client_options=client_options)
    await __delete_via_client(
        "_delete_metadata_store",
        "metadata store",
        client.delete_metadata_store,
        resource,
        prefix,
    )


class AIPlatformDeleteHandler(BaseDeleteHandler):
    DELETERS = {
        "aiplatform.googleapis.com/Dataset": _delete_dataset,
        "aiplatform.googleapis.com/BatchPredictionJob": _delete_batch_prediction_job,
        "aiplatform.googleapis.com/CustomJob": _delete_custom_job,
        "aiplatform.googleapis.com/HyperparameterTuningJob": _delete_hyperparameter_tuning_job,
        "aiplatform.googleapis.com/Model": _delete_model,
        "aiplatform.googleapis.com/TrainingPipeline": _delete_training_pipeline,
        "aiplatform.googleapis.com/Endpoint": _delete_endpoint,
        "aiplatform.googleapis.com/FeatureOnlineStore": _delete_feature_online_store,
        "aiplatform.googleapis.com/CachedContent": _delete_cached_content,
        "aiplatform.googleapis.com/PipelineJob": _delete_pipeline_job,
        "aiplatform.googleapis.com/Tensorboard": _delete_tensorboard,
        "aiplatform.googleapis.com/TuningJob": _delete_tuning_job,
        "aiplatform.googleapis.com/MetadataStore": _delete_metadata_store,
    }

    DELETION_ORDER = [
        "aiplatform.googleapis.com/Model",
        "aiplatform.googleapis.com/Endpoint",
        "aiplatform.googleapis.com/Dataset",
        "aiplatform.googleapis.com/TrainingPipeline",
        "aiplatform.googleapis.com/PipelineJob",
        "aiplatform.googleapis.com/TuningJob",
        "aiplatform.googleapis.com/BatchPredictionJob",
        "aiplatform.googleapis.com/HyperparameterTuningJob",
        "aiplatform.googleapis.com/FeatureOnlineStore",
        "aiplatform.googleapis.com/CachedContent",
        "aiplatform.googleapis.com/Tensorboard",
        "aiplatform.googleapis.com/CustomJob",
        "aiplatform.googleapis.com/MetadataStore",
    ]

    SLEEP_AFTER_EACH_REQUEST = 0.01  # 3
