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
"""This module contains helper functions for MLEngine operators."""

from __future__ import annotations

import base64
import json
import os
import re
from typing import TYPE_CHECKING, Callable, Iterable, TypeVar
from urllib.parse import urlsplit

import dill

from airflow.exceptions import AirflowException
from airflow.providers.apache.beam.hooks.beam import BeamRunnerType
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.common.compat.standard.operators import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.mlengine import MLEngineStartBatchPredictionJobOperator

if TYPE_CHECKING:
    from airflow import DAG

T = TypeVar("T", bound=Callable)


def create_evaluate_ops(
    task_prefix: str,
    data_format: str,
    input_paths: list[str],
    prediction_path: str,
    metric_fn_and_keys: tuple[T, Iterable[str]],
    validate_fn: T,
    batch_prediction_job_id: str | None = None,
    region: str | None = None,
    project_id: str | None = None,
    dataflow_options: dict | None = None,
    model_uri: str | None = None,
    model_name: str | None = None,
    version_name: str | None = None,
    dag: DAG | None = None,
    py_interpreter="python3",
) -> tuple[MLEngineStartBatchPredictionJobOperator, BeamRunPythonPipelineOperator, PythonOperator]:
    r"""
    Create Operators needed for model evaluation and returns.

    This function is deprecated. All the functionality of legacy MLEngine and new features are available
    on the Vertex AI platform.

    To create and view Model Evaluation, please check the documentation:
    https://cloud.google.com/vertex-ai/docs/evaluation/using-model-evaluation#create_an_evaluation.

    It gets prediction over inputs via Cloud ML Engine BatchPrediction API by
    calling MLEngineBatchPredictionOperator, then summarize and validate
    the result via Cloud Dataflow using DataFlowPythonOperator.

    For details and pricing about Batch prediction, please refer to the website
    https://cloud.google.com/ml-engine/docs/how-tos/batch-predict
    and for Cloud Dataflow, https://cloud.google.com/dataflow/docs/

    It returns three chained operators for prediction, summary, and validation,
    named as ``<prefix>-prediction``, ``<prefix>-summary``, and ``<prefix>-validation``,
    respectively.
    (``<prefix>`` should contain only alphanumeric characters or hyphen.)

    The upstream and downstream can be set accordingly like:

    .. code-block:: python

        pred, _, val = create_evaluate_ops(...)
        pred.set_upstream(upstream_op)
        ...
        downstream_op.set_upstream(val)

    Callers will provide two python callables, metric_fn and validate_fn, in
    order to customize the evaluation behavior as they wish.

    - metric_fn receives a dictionary per instance derived from json in the
      batch prediction result. The keys might vary depending on the model.
      It should return a tuple of metrics.
    - validation_fn receives a dictionary of the averaged metrics that metric_fn
      generated over all instances.
      The key/value of the dictionary matches to what's given by
      metric_fn_and_keys arg.
      The dictionary contains an additional metric, 'count' to represent the
      total number of instances received for evaluation.
      The function would raise an exception to mark the task as failed, in a
      case the validation result is not okay to proceed (i.e. to set the trained
      version as default).

    Typical examples are like this:

    .. code-block:: python

        def get_metric_fn_and_keys():
            import math  # imports should be outside of the metric_fn below.

            def error_and_squared_error(inst):
                label = float(inst["input_label"])
                classes = float(inst["classes"])  # 0 or 1
                err = abs(classes - label)
                squared_err = math.pow(classes - label, 2)
                return (err, squared_err)  # returns a tuple.

            return error_and_squared_error, ["err", "mse"]  # key order must match.


        def validate_err_and_count(summary):
            if summary["err"] > 0.2:
                raise ValueError("Too high err>0.2; summary=%s" % summary)
            if summary["mse"] > 0.05:
                raise ValueError("Too high mse>0.05; summary=%s" % summary)
            if summary["count"] < 1000:
                raise ValueError("Too few instances<1000; summary=%s" % summary)
            return summary

    For the details on the other BatchPrediction-related arguments (project_id,
    job_id, region, data_format, input_paths, prediction_path, model_uri),
    please refer to MLEngineBatchPredictionOperator too.

    :param task_prefix: a prefix for the tasks. Only alphanumeric characters and
        hyphen are allowed (no underscores), since this will be used as dataflow
        job name, which doesn't allow other characters.

    :param data_format: either of 'TEXT', 'TF_RECORD', 'TF_RECORD_GZIP'

    :param input_paths: a list of input paths to be sent to BatchPrediction.

    :param prediction_path: GCS path to put the prediction results in.

    :param metric_fn_and_keys: a tuple of metric_fn and metric_keys:

        - metric_fn is a function that accepts a dictionary (for an instance),
          and returns a tuple of metric(s) that it calculates.

        - metric_keys is a list of strings to denote the key of each metric.

    :param validate_fn: a function to validate whether the averaged metric(s) is
        good enough to push the model.

    :param batch_prediction_job_id: the id to use for the Cloud ML Batch
        prediction job. Passed directly to the MLEngineBatchPredictionOperator as
        the job_id argument.

    :param project_id: the Google Cloud project id in which to execute
        Cloud ML Batch Prediction and Dataflow jobs. If None, then the `dag`'s
        `default_args['project_id']` will be used.

    :param region: the Google Cloud region in which to execute Cloud ML
        Batch Prediction and Dataflow jobs. If None, then the `dag`'s
        `default_args['region']` will be used.

    :param dataflow_options: options to run Dataflow jobs. If None, then the
        `dag`'s `default_args['dataflow_default_options']` will be used.

    :param model_uri: GCS path of the model exported by Tensorflow using
        ``tensorflow.estimator.export_savedmodel()``. It cannot be used with
        model_name or version_name below. See MLEngineBatchPredictionOperator for
        more detail.

    :param model_name: Used to indicate a model to use for prediction. Can be
        used in combination with version_name, but cannot be used together with
        model_uri. See MLEngineBatchPredictionOperator for more detail. If None,
        then the `dag`'s `default_args['model_name']` will be used.

    :param version_name: Used to indicate a model version to use for prediction,
        in combination with model_name. Cannot be used together with model_uri.
        See MLEngineBatchPredictionOperator for more detail. If None, then the
        `dag`'s `default_args['version_name']` will be used.

    :param dag: The `DAG` to use for all Operators.

    :param py_interpreter: Python version of the beam pipeline.
        If None, this defaults to the python3.
        To track python versions supported by beam and related
        issues check: https://issues.apache.org/jira/browse/BEAM-1251

    :returns: a tuple of three operators, (prediction, summary, validation)
                  PythonOperator)
    """
    batch_prediction_job_id = batch_prediction_job_id or ""
    dataflow_options = dataflow_options or {}
    region = region or ""

    # Verify that task_prefix doesn't have any special characters except hyphen
    # '-', which is the only allowed non-alphanumeric character by Dataflow.
    if not re.fullmatch(r"[a-zA-Z][-A-Za-z0-9]*", task_prefix):
        raise AirflowException(
            "Malformed task_id for DataFlowPythonOperator (only alphanumeric "
            "and hyphens are allowed but got: " + task_prefix
        )

    metric_fn, metric_keys = metric_fn_and_keys
    if not callable(metric_fn):
        raise AirflowException("`metric_fn` param must be callable.")
    if not callable(validate_fn):
        raise AirflowException("`validate_fn` param must be callable.")

    if dag is not None and dag.default_args is not None:
        default_args = dag.default_args
        project_id = project_id or default_args.get("project_id")
        region = region or default_args["region"]
        model_name = model_name or default_args.get("model_name")
        version_name = version_name or default_args.get("version_name")
        dataflow_options = dataflow_options or default_args.get("dataflow_default_options")

    evaluate_prediction = MLEngineStartBatchPredictionJobOperator(
        task_id=(task_prefix + "-prediction"),
        project_id=project_id,
        job_id=batch_prediction_job_id,
        region=region,
        data_format=data_format,
        input_paths=input_paths,
        output_path=prediction_path,
        uri=model_uri,
        model_name=model_name,
        version_name=version_name,
        dag=dag,
    )

    metric_fn_encoded = base64.b64encode(dill.dumps(metric_fn, recurse=True)).decode()
    evaluate_summary = BeamRunPythonPipelineOperator(
        task_id=(task_prefix + "-summary"),
        runner=BeamRunnerType.DataflowRunner,
        py_file=os.path.join(os.path.dirname(__file__), "mlengine_prediction_summary.py"),
        default_pipeline_options=dataflow_options,
        pipeline_options={
            "prediction_path": prediction_path,
            "metric_fn_encoded": metric_fn_encoded,
            "metric_keys": ",".join(metric_keys),
        },
        py_interpreter=py_interpreter,
        py_requirements=["apache-beam[gcp]>=2.46.0"],
        dag=dag,
    )
    evaluate_summary.set_upstream(evaluate_prediction)

    def apply_validate_fn(*args, templates_dict, **kwargs):
        prediction_path = templates_dict["prediction_path"]
        scheme, bucket, obj, _, _ = urlsplit(prediction_path)
        if scheme != "gs" or not bucket or not obj:
            raise ValueError(f"Wrong format prediction_path: {prediction_path}")
        summary = os.path.join(obj.strip("/"), "prediction.summary.json")
        gcs_hook = GCSHook()
        summary = json.loads(gcs_hook.download(bucket, summary).decode("utf-8"))
        return validate_fn(summary)

    evaluate_validation = PythonOperator(
        task_id=(task_prefix + "-validation"),
        python_callable=apply_validate_fn,
        templates_dict={"prediction_path": prediction_path},
        dag=dag,
    )
    evaluate_validation.set_upstream(evaluate_summary)

    return evaluate_prediction, evaluate_summary, evaluate_validation
