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

# mypy ignore arg types (for templated fields)
# type: ignore[arg-type]

"""
Example Airflow DAG for Google Vertex AI service testing Auto ML operations.
"""
import os
from datetime import datetime
from pathlib import Path

from google.cloud.aiplatform import schema
from google.protobuf.json_format import ParseDict
from google.protobuf.struct_pb2 import Value

from airflow import models
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.operators.vertex_ai.auto_ml import (
    CreateAutoMLForecastingTrainingJobOperator,
    CreateAutoMLImageTrainingJobOperator,
    CreateAutoMLTabularTrainingJobOperator,
    CreateAutoMLTextTrainingJobOperator,
    CreateAutoMLVideoTrainingJobOperator,
    DeleteAutoMLTrainingJobOperator,
    ListAutoMLTrainingJobOperator,
)
from airflow.providers.google.cloud.operators.vertex_ai.dataset import (
    CreateDatasetOperator,
    DeleteDatasetOperator,
    ImportDataOperator,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "vertex_ai_auto_ml_operations"
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")
REGION = "us-central1"

FORECAST_GCS_BUCKET_NAME = f"bucket_forecast_{DAG_ID}_{ENV_ID}"
IMAGE_GCS_BUCKET_NAME = f"bucket_image_{DAG_ID}_{ENV_ID}"
TABULAR_GCS_BUCKET_NAME = f"bucket_tabular_{DAG_ID}_{ENV_ID}"
TEXT_GCS_BUCKET_NAME = f"bucket_text_{DAG_ID}_{ENV_ID}"
VIDEO_GCS_BUCKET_NAME = f"bucket_custom_python_{DAG_ID}_{ENV_ID}"

FORECAST_GCS_OBJECT_NAME = "vertex-ai/forecast-dataset.csv"
IMAGE_GCS_OBJECT_NAME = "vertex-ai/image-dataset.csv"
TABULAR_GCS_OBJECT_NAME = "vertex-ai/tabular-dataset.csv"
TEXT_GCS_OBJECT_NAME = "vertex-ai/text-dataset.csv"
VIDEO_GCS_OBJECT_NAME = "vertex-ai/video-dataset.csv"

FORECAST_CSV_FILE_LOCAL_PATH = (str(Path(__file__).parent / "resources" / "forecast-dataset.csv"),)
IMAGE_CSV_FILE_LOCAL_PATH = (str(Path(__file__).parent / "resources" / "image-dataset.csv"),)
TABULAR_CSV_FILE_LOCAL_PATH = (str(Path(__file__).parent / "resources" / "tabular-dataset.csv"),)
TEXT_CSV_FILE_LOCAL_PATH = (str(Path(__file__).parent / "resources" / "text-dataset.csv"),)
VIDEO_CSV_FILE_LOCAL_PATH = (str(Path(__file__).parent / "resources" / "video-dataset.csv"),)

FORECAST_DATASET = {
    "display_name": f"forecast-dataset-{ENV_ID}",
    "metadata_schema_uri": schema.dataset.metadata.time_series,
    "metadata": ParseDict(
        {
            "input_config": {
                "gcs_source": {"uri": [f"gs://{FORECAST_GCS_BUCKET_NAME}/vertex-ai/forecast-dataset.csv"]}
            }
        },
        Value(),
    ),
}
IMAGE_DATASET = {
    "display_name": f"image-dataset-{ENV_ID}",
    "metadata_schema_uri": schema.dataset.metadata.image,
    "metadata": Value(string_value="image-dataset"),
}
TABULAR_DATASET = {
    "display_name": f"tabular-dataset-{ENV_ID}",
    "metadata_schema_uri": schema.dataset.metadata.tabular,
    "metadata": ParseDict(
        {
            "input_config": {
                "gcs_source": {"uri": [f"gs://{TABULAR_GCS_BUCKET_NAME}/vertex-ai/tabular-dataset.csv"]}
            }
        },
        Value(),
    ),
}
TEXT_DATASET = {
    "display_name": f"text-dataset-{ENV_ID}",
    "metadata_schema_uri": schema.dataset.metadata.text,
    "metadata": Value(string_value="text-dataset"),
}
VIDEO_DATASET = {
    "display_name": f"video-dataset-{ENV_ID}",
    "metadata_schema_uri": schema.dataset.metadata.video,
    "metadata": Value(string_value="video-dataset"),
}

IMAGE_DATA_CONFIG = [
    {
        "import_schema_uri": schema.dataset.ioformat.image.single_label_classification,
        "gcs_source": {"uris": [f"gs://{IMAGE_GCS_BUCKET_NAME}/vertex-ai/image-dataset.csv"]},
    },
]
TEXT_DATA_CONFIG = [
    {
        "import_schema_uri": schema.dataset.ioformat.text.single_label_classification,
        "gcs_source": {"uris": [f"gs://{TEXT_GCS_BUCKET_NAME}/vertex-ai/text-dataset.csv"]},
    },
]
VIDEO_DATA_CONFIG = [
    {
        "import_schema_uri": schema.dataset.ioformat.video.classification,
        "gcs_source": {"uris": [f"gs://{VIDEO_GCS_OBJECT_NAME}/vertex-ai/video-dataset.csv"]},
    },
]

TEST_TIME_COLUMN = "date"
TEST_TIME_SERIES_IDENTIFIER_COLUMN = "store_name"
TEST_TARGET_COLUMN = "sale_dollars"

COLUMN_SPECS = {
    TEST_TIME_COLUMN: "timestamp",
    TEST_TARGET_COLUMN: "numeric",
    "city": "categorical",
    "zip_code": "categorical",
    "county": "categorical",
}

COLUMN_TRANSFORMATIONS = [
    {"categorical": {"column_name": "Type"}},
    {"numeric": {"column_name": "Age"}},
    {"categorical": {"column_name": "Breed1"}},
    {"categorical": {"column_name": "Color1"}},
    {"categorical": {"column_name": "Color2"}},
    {"categorical": {"column_name": "MaturitySize"}},
    {"categorical": {"column_name": "FurLength"}},
    {"categorical": {"column_name": "Vaccinated"}},
    {"categorical": {"column_name": "Sterilized"}},
    {"categorical": {"column_name": "Health"}},
    {"numeric": {"column_name": "Fee"}},
    {"numeric": {"column_name": "PhotoAmt"}},
]


with models.DAG(
    f"{DAG_ID}_forecasting_training_job",
    schedule_interval="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "vertex_ai", "auto_ml"],
) as forecasting_training_job_dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=FORECAST_GCS_BUCKET_NAME,
        storage_class="REGIONAL",
        location=REGION,
    )
    upload_files = LocalFilesystemToGCSOperator(
        task_id="upload_file_to_bucket",
        src=FORECAST_CSV_FILE_LOCAL_PATH,
        dst=FORECAST_GCS_OBJECT_NAME,
        bucket=FORECAST_GCS_BUCKET_NAME,
    )

    create_forecast_dataset = CreateDatasetOperator(
        task_id="forecast_dataset",
        dataset=FORECAST_DATASET,
        region=REGION,
        project_id=PROJECT_ID,
    )
    forecast_dataset_id = create_forecast_dataset.output['dataset_id']

    # [START how_to_cloud_vertex_ai_create_auto_ml_forecasting_training_job_operator]
    create_auto_ml_forecasting_training_job = CreateAutoMLForecastingTrainingJobOperator(
        task_id="auto_ml_forecasting_task",
        display_name=f"auto-ml-forecasting-{ENV_ID}",
        optimization_objective="minimize-rmse",
        column_specs=COLUMN_SPECS,
        # run params
        dataset_id=forecast_dataset_id,
        target_column=TEST_TARGET_COLUMN,
        time_column=TEST_TIME_COLUMN,
        time_series_identifier_column=TEST_TIME_SERIES_IDENTIFIER_COLUMN,
        available_at_forecast_columns=[TEST_TIME_COLUMN],
        unavailable_at_forecast_columns=[TEST_TARGET_COLUMN],
        time_series_attribute_columns=["city", "zip_code", "county"],
        forecast_horizon=30,
        context_window=30,
        data_granularity_unit="day",
        data_granularity_count=1,
        weight_column=None,
        budget_milli_node_hours=1000,
        model_display_name=f"auto-ml-forecasting-model-{ENV_ID}",
        predefined_split_column_name=None,
        region=REGION,
        project_id=PROJECT_ID,
    )
    # [END how_to_cloud_vertex_ai_create_auto_ml_forecasting_training_job_operator]

    # [START how_to_cloud_vertex_ai_delete_auto_ml_training_job_operator]
    delete_auto_ml_forecasting_training_job = DeleteAutoMLTrainingJobOperator(
        task_id="delete_auto_ml_forecasting_training_job",
        training_pipeline_id=create_auto_ml_forecasting_training_job.output['training_id'],
        region=REGION,
        project_id=PROJECT_ID,
    )
    # [END how_to_cloud_vertex_ai_delete_auto_ml_training_job_operator]

    delete_forecast_dataset = DeleteDatasetOperator(
        task_id="delete_forecast_dataset",
        dataset_id=forecast_dataset_id,
        region=REGION,
        project_id=PROJECT_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=FORECAST_GCS_BUCKET_NAME, trigger_rule=TriggerRule.ALL_DONE
    )

    (
        # TEST SETUP
        create_bucket
        >> upload_files
        >> create_forecast_dataset
        # TEST BODY
        >> create_auto_ml_forecasting_training_job
        # TEST TEARDOWN
        >> delete_auto_ml_forecasting_training_job
        >> delete_forecast_dataset
        >> delete_bucket
    )


with models.DAG(
    f"{DAG_ID}_image_training_job",
    schedule_interval="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "vertex_ai", "auto_ml"],
) as image_training_job_dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=IMAGE_GCS_BUCKET_NAME,
        storage_class="REGIONAL",
        location=REGION,
    )
    upload_files = LocalFilesystemToGCSOperator(
        task_id="upload_file_to_bucket",
        src=IMAGE_CSV_FILE_LOCAL_PATH,
        dst=IMAGE_GCS_OBJECT_NAME,
        bucket=IMAGE_GCS_BUCKET_NAME,
    )

    create_image_dataset = CreateDatasetOperator(
        task_id="image_dataset",
        dataset=IMAGE_DATASET,
        region=REGION,
        project_id=PROJECT_ID,
    )
    image_dataset_id = create_image_dataset.output['dataset_id']

    import_image_dataset = ImportDataOperator(
        task_id="import_image_data",
        dataset_id=image_dataset_id,
        region=REGION,
        project_id=PROJECT_ID,
        import_configs=IMAGE_DATA_CONFIG,
    )

    # [START how_to_cloud_vertex_ai_create_auto_ml_image_training_job_operator]
    create_auto_ml_image_training_job = CreateAutoMLImageTrainingJobOperator(
        task_id="auto_ml_image_task",
        display_name=f"auto-ml-image-{ENV_ID}",
        dataset_id=image_dataset_id,
        prediction_type="classification",
        multi_label=False,
        model_type="CLOUD",
        training_fraction_split=0.6,
        validation_fraction_split=0.2,
        test_fraction_split=0.2,
        budget_milli_node_hours=8000,
        model_display_name=f"auto-ml-image-model-{ENV_ID}",
        disable_early_stopping=False,
        region=REGION,
        project_id=PROJECT_ID,
    )
    # [END how_to_cloud_vertex_ai_create_auto_ml_image_training_job_operator]

    delete_auto_ml_image_training_job = DeleteAutoMLTrainingJobOperator(
        task_id="delete_auto_ml_training_job",
        training_pipeline_id=create_auto_ml_image_training_job.output['training_id'],
        region=REGION,
        project_id=PROJECT_ID,
    )

    delete_image_dataset = DeleteDatasetOperator(
        task_id="delete_image_dataset",
        dataset_id=image_dataset_id,
        region=REGION,
        project_id=PROJECT_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=IMAGE_GCS_BUCKET_NAME, trigger_rule=TriggerRule.ALL_DONE
    )

    (
        # TEST SETUP
        [
            create_bucket,
            create_image_dataset,
        ]
        >> upload_files
        >> import_image_dataset
        # TEST BODY
        >> create_auto_ml_image_training_job
        # TEST TEARDOWN
        >> delete_auto_ml_image_training_job
        >> delete_image_dataset
        >> delete_bucket
    )


with models.DAG(
    f"{DAG_ID}_tabular_training_job",
    schedule_interval="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "vertex_ai", "auto_ml"],
) as tabular_training_job_dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=TABULAR_GCS_BUCKET_NAME,
        storage_class="REGIONAL",
        location=REGION,
    )
    upload_files = LocalFilesystemToGCSOperator(
        task_id="upload_file_to_bucket",
        src=TABULAR_CSV_FILE_LOCAL_PATH,
        dst=TABULAR_GCS_OBJECT_NAME,
        bucket=TABULAR_GCS_BUCKET_NAME,
    )

    create_tabular_dataset = CreateDatasetOperator(
        task_id="tabular_dataset",
        dataset=TABULAR_DATASET,
        region=REGION,
        project_id=PROJECT_ID,
    )
    tabular_dataset_id = create_tabular_dataset.output['dataset_id']

    # [START how_to_cloud_vertex_ai_create_auto_ml_tabular_training_job_operator]
    create_auto_ml_tabular_training_job = CreateAutoMLTabularTrainingJobOperator(
        task_id="auto_ml_tabular_task",
        display_name=f"auto-ml-tabular-{ENV_ID}",
        optimization_prediction_type="classification",
        column_transformations=COLUMN_TRANSFORMATIONS,
        dataset_id=tabular_dataset_id,
        target_column="Adopted",
        training_fraction_split=0.8,
        validation_fraction_split=0.1,
        test_fraction_split=0.1,
        model_display_name="adopted-prediction-model",
        disable_early_stopping=False,
        region=REGION,
        project_id=PROJECT_ID,
    )
    # [END how_to_cloud_vertex_ai_create_auto_ml_tabular_training_job_operator]

    delete_auto_ml_tabular_training_job = DeleteAutoMLTrainingJobOperator(
        task_id="delete_auto_ml_training_job",
        training_pipeline_id=create_auto_ml_tabular_training_job.output['training_id'],
        region=REGION,
        project_id=PROJECT_ID,
    )

    delete_tabular_dataset = DeleteDatasetOperator(
        task_id="delete_tabular_dataset",
        dataset_id=tabular_dataset_id,
        region=REGION,
        project_id=PROJECT_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=TABULAR_GCS_BUCKET_NAME, trigger_rule=TriggerRule.ALL_DONE
    )

    (
        # TEST SETUP
        create_bucket
        >> upload_files
        >> create_tabular_dataset
        # TEST BODY
        >> create_auto_ml_tabular_training_job
        # TEST TEARDOWN
        >> delete_auto_ml_tabular_training_job
        >> delete_tabular_dataset
        >> delete_bucket
    )


with models.DAG(
    f"{DAG_ID}_text_training_job",
    schedule_interval="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "vertex_ai", "auto_ml"],
) as text_training_job_dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=TEXT_GCS_BUCKET_NAME,
        storage_class="REGIONAL",
        location=REGION,
    )
    upload_files = LocalFilesystemToGCSOperator(
        task_id="upload_file_to_bucket",
        src=TEXT_CSV_FILE_LOCAL_PATH,
        dst=TEXT_GCS_OBJECT_NAME,
        bucket=TEXT_GCS_BUCKET_NAME,
    )

    create_text_dataset = CreateDatasetOperator(
        task_id="text_dataset",
        dataset=TEXT_DATASET,
        region=REGION,
        project_id=PROJECT_ID,
    )
    text_dataset_id = create_text_dataset.output['dataset_id']

    import_text_dataset = ImportDataOperator(
        task_id="import_text_data",
        dataset_id=text_dataset_id,
        region=REGION,
        project_id=PROJECT_ID,
        import_configs=TEXT_DATA_CONFIG,
    )

    # [START how_to_cloud_vertex_ai_create_auto_ml_text_training_job_operator]
    create_auto_ml_text_training_job = CreateAutoMLTextTrainingJobOperator(
        task_id="auto_ml_text_task",
        display_name=f"auto-ml-text-{ENV_ID}",
        prediction_type="classification",
        multi_label=False,
        dataset_id=text_dataset_id,
        model_display_name=f"auto-ml-text-model-{ENV_ID}",
        training_fraction_split=0.7,
        validation_fraction_split=0.2,
        test_fraction_split=0.1,
        sync=True,
        region=REGION,
        project_id=PROJECT_ID,
    )
    # [END how_to_cloud_vertex_ai_create_auto_ml_text_training_job_operator]

    delete_auto_ml_text_training_job = DeleteAutoMLTrainingJobOperator(
        task_id="delete_auto_ml_text_training_job",
        training_pipeline_id=create_auto_ml_text_training_job.output['training_id'],
        region=REGION,
        project_id=PROJECT_ID,
    )

    delete_text_dataset = DeleteDatasetOperator(
        task_id="delete_text_dataset",
        dataset_id=text_dataset_id,
        region=REGION,
        project_id=PROJECT_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=TEXT_GCS_BUCKET_NAME, trigger_rule=TriggerRule.ALL_DONE
    )

    (
        # TEST SETUP
        [
            create_bucket,
            create_text_dataset,
        ]
        >> upload_files
        >> import_text_dataset
        # TEST BODY
        >> create_auto_ml_text_training_job
        # TEST TEARDOWN
        >> delete_auto_ml_text_training_job
        >> delete_text_dataset
        >> delete_bucket
    )


with models.DAG(
    f"{DAG_ID}_video_training_job",
    schedule_interval="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "vertex_ai", "auto_ml"],
) as video_training_job_dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=VIDEO_GCS_BUCKET_NAME,
        storage_class="REGIONAL",
        location=REGION,
    )
    upload_files = LocalFilesystemToGCSOperator(
        task_id="upload_file_to_bucket",
        src=VIDEO_CSV_FILE_LOCAL_PATH,
        dst=VIDEO_GCS_OBJECT_NAME,
        bucket=VIDEO_GCS_BUCKET_NAME,
    )

    create_video_dataset = CreateDatasetOperator(
        task_id="video_dataset",
        dataset=VIDEO_DATASET,
        region=REGION,
        project_id=PROJECT_ID,
    )
    video_dataset_id = create_video_dataset.output['dataset_id']

    import_video_dataset = ImportDataOperator(
        task_id="import_video_data",
        dataset_id=video_dataset_id,
        region=REGION,
        project_id=PROJECT_ID,
        import_configs=VIDEO_DATA_CONFIG,
    )

    # [START how_to_cloud_vertex_ai_create_auto_ml_video_training_job_operator]
    create_auto_ml_video_training_job = CreateAutoMLVideoTrainingJobOperator(
        task_id="auto_ml_video_task",
        display_name=f"auto-ml-video-{ENV_ID}",
        prediction_type="classification",
        model_type="CLOUD",
        dataset_id=video_dataset_id,
        model_display_name=f"auto-ml-video-model-{ENV_ID}",
        region=REGION,
        project_id=PROJECT_ID,
    )
    # [END how_to_cloud_vertex_ai_create_auto_ml_video_training_job_operator]

    delete_auto_ml_video_training_job = DeleteAutoMLTrainingJobOperator(
        task_id="delete_auto_ml_video_training_job",
        training_pipeline_id=create_auto_ml_video_training_job.output['training_id'],
        region=REGION,
        project_id=PROJECT_ID,
    )

    delete_video_dataset = DeleteDatasetOperator(
        task_id="delete_video_dataset",
        dataset_id=video_dataset_id,
        region=REGION,
        project_id=PROJECT_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=VIDEO_GCS_BUCKET_NAME, trigger_rule=TriggerRule.ALL_DONE
    )

    (
        # TEST SETUP
        [
            create_bucket,
            create_video_dataset,
        ]
        >> upload_files
        >> import_video_dataset
        # TEST BODY
        >> create_auto_ml_video_training_job
        # TEST TEARDOWN
        >> delete_auto_ml_video_training_job
        >> delete_video_dataset
        >> delete_bucket
    )


with models.DAG(
    f"{DAG_ID}_list_training_job",
    schedule_interval="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "vertex_ai", "auto_ml", "list_operation"],
) as list_training_job_dag:

    # [START how_to_cloud_vertex_ai_list_auto_ml_training_job_operator]
    list_auto_ml_training_job = ListAutoMLTrainingJobOperator(
        task_id="list_auto_ml_training_job",
        region=REGION,
        project_id=PROJECT_ID,
    )
    # [END how_to_cloud_vertex_ai_list_auto_ml_training_job_operator]


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
forecast_test_run = get_test_run(forecasting_training_job_dag)
image_test_run = get_test_run(image_training_job_dag)
tabular_test_run = get_test_run(tabular_training_job_dag)
text_test_run = get_test_run(text_training_job_dag)
video_test_run = get_test_run(video_training_job_dag)
list_test_run = get_test_run(list_training_job_dag)
