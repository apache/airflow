#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the 'License'); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
This module contains helper functions for MLEngine operators.
"""

import base64
import json
import os
import re
from urllib.parse import urlsplit

import dill

from airflow.gcp.hooks.gcs import GoogleCloudStorageHook
from airflow.gcp.operators.mlengine import MLEngineBatchPredictionOperator
from airflow.gcp.operators.dataflow import DataFlowPythonOperator
from airflow.exceptions import AirflowException
from airflow.operators.python_operator import PythonOperator


def create_evaluate_ops(task_prefix,  # pylint:disable=too-many-arguments
                        data_format,
                        input_paths,
                        prediction_path,
                        metric_fn_and_keys,
                        validate_fn,
                        batch_prediction_job_id=None,
                        project_id=None,
                        region=None,
                        dataflow_options=None,
                        model_uri=None,
                        model_name=None,
                        version_name=None,
                        dag=None):
    """
    Creates Operators needed for model evaluation and returns.

    It gets prediction over inputs via Cloud ML Engine BatchPrediction API by
    calling MLEngineBatchPredictionOperator, then summarize and validate
    the result via Cloud Dataflow using DataFlowPythonOperator.

    For details and pricing about Batch prediction, please refer to the website
    https://cloud.google.com/ml-engine/docs/how-tos/batch-predict
    and for Cloud Dataflow, https://cloud.google.com/dataflow/docs/

    It returns three chained operators for prediction, summary, and validation,
    named as <prefix>-prediction, <prefix>-summary, and <prefix>-validation,
    respectively.
    (<prefix> should contain only alphanumeric characters or hyphen.)

    The upstream and downstream can be set accordingly like:
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

    def get_metric_fn_and_keys():
        import math  # imports should be outside of the metric_fn below.
        def error_and_squared_error(inst):
            label = float(inst['input_label'])
            classes = float(inst['classes'])  # 0 or 1
            err = abs(classes-label)
            squared_err = math.pow(classes-label, 2)
            return (err, squared_err)  # returns a tuple.
        return error_and_squared_error, ['err', 'mse']  # key order must match.

    def validate_err_and_count(summary):
        if summary['err'] > 0.2:
            raise ValueError('Too high err>0.2; summary=%s' % summary)
        if summary['mse'] > 0.05:
            raise ValueError('Too high mse>0.05; summary=%s' % summary)
        if summary['count'] < 1000:
            raise ValueError('Too few instances<1000; summary=%s' % summary)
        return summary

    For the details on the other BatchPrediction-related arguments (project_id,
    job_id, region, data_format, input_paths, prediction_path, model_uri),
    please refer to MLEngineBatchPredictionOperator too.

    :param task_prefix: a prefix for the tasks. Only alphanumeric characters and
        hyphen are allowed (no underscores), since this will be used as dataflow
        job name, which doesn't allow other characters.
    :type task_prefix: str

    :param data_format: either of 'TEXT', 'TF_RECORD', 'TF_RECORD_GZIP'
    :type data_format: str

    :param input_paths: a list of input paths to be sent to BatchPrediction.
    :type input_paths: list[str]

    :param prediction_path: GCS path to put the prediction results in.
    :type prediction_path: str

    :param metric_fn_and_keys: a tuple of metric_fn and metric_keys:
        - metric_fn is a function that accepts a dictionary (for an instance),
          and returns a tuple of metric(s) that it calculates.
        - metric_keys is a list of strings to denote the key of each metric.
    :type metric_fn_and_keys: tuple of a function and a list[str]

    :param validate_fn: a function to validate whether the averaged metric(s) is
        good enough to push the model.
    :type validate_fn: function

    :param batch_prediction_job_id: the id to use for the Cloud ML Batch
        prediction job. Passed directly to the MLEngineBatchPredictionOperator as
        the job_id argument.
    :type batch_prediction_job_id: str

    :param project_id: the Google Cloud Platform project id in which to execute
        Cloud ML Batch Prediction and Dataflow jobs. If None, then the `dag`'s
        `default_args['project_id']` will be used.
    :type project_id: str

    :param region: the Google Cloud Platform region in which to execute Cloud ML
        Batch Prediction and Dataflow jobs. If None, then the `dag`'s
        `default_args['region']` will be used.
    :type region: str

    :param dataflow_options: options to run Dataflow jobs. If None, then the
        `dag`'s `default_args['dataflow_default_options']` will be used.
    :type dataflow_options: dictionary

    :param model_uri: GCS path of the model exported by Tensorflow using
        tensorflow.estimator.export_savedmodel(). It cannot be used with
        model_name or version_name below. See MLEngineBatchPredictionOperator for
        more detail.
    :type model_uri: str

    :param model_name: Used to indicate a model to use for prediction. Can be
        used in combination with version_name, but cannot be used together with
        model_uri. See MLEngineBatchPredictionOperator for more detail. If None,
        then the `dag`'s `default_args['model_name']` will be used.
    :type model_name: str

    :param version_name: Used to indicate a model version to use for prediction,
        in combination with model_name. Cannot be used together with model_uri.
        See MLEngineBatchPredictionOperator for more detail. If None, then the
        `dag`'s `default_args['version_name']` will be used.
    :type version_name: str

    :param dag: The `DAG` to use for all Operators.
    :type dag: airflow.models.DAG

    :returns: a tuple of three operators, (prediction, summary, validation)
    :rtype: tuple(DataFlowPythonOperator, DataFlowPythonOperator,
                  PythonOperator)
    """

    # Verify that task_prefix doesn't have any special characters except hyphen
    # '-', which is the only allowed non-alphanumeric character by Dataflow.
    if not re.match(r"^[a-zA-Z][-A-Za-z0-9]*$", task_prefix):
        raise AirflowException(
            "Malformed task_id for DataFlowPythonOperator (only alphanumeric "
            "and hyphens are allowed but got: " + task_prefix)

    metric_fn, metric_keys = metric_fn_and_keys
    if not callable(metric_fn):
        raise AirflowException("`metric_fn` param must be callable.")
    if not callable(validate_fn):
        raise AirflowException("`validate_fn` param must be callable.")

    if dag is not None and dag.default_args is not None:
        default_args = dag.default_args
        project_id = project_id or default_args.get('project_id')
        region = region or default_args.get('region')
        model_name = model_name or default_args.get('model_name')
        version_name = version_name or default_args.get('version_name')
        dataflow_options = dataflow_options or \
            default_args.get('dataflow_default_options')

    evaluate_prediction = MLEngineBatchPredictionOperator(
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
        dag=dag)

    metric_fn_encoded = base64.b64encode(dill.dumps(metric_fn, recurse=True))
    evaluate_summary = DataFlowPythonOperator(
        task_id=(task_prefix + "-summary"),
        py_options=["-m"],
        py_file="airflow.gcp.utils.mlengine_prediction_summary",
        dataflow_default_options=dataflow_options,
        options={
            "prediction_path": prediction_path,
            "metric_fn_encoded": metric_fn_encoded,
            "metric_keys": ','.join(metric_keys)
        },
        py_interpreter='python2',
        dag=dag)
    evaluate_summary.set_upstream(evaluate_prediction)

    def apply_validate_fn(*args, **kwargs):
        prediction_path = kwargs["templates_dict"]["prediction_path"]
        scheme, bucket, obj, _, _ = urlsplit(prediction_path)
        if scheme != "gs" or not bucket or not obj:
            raise ValueError("Wrong format prediction_path: {}".format(prediction_path))
        summary = os.path.join(obj.strip("/"),
                               "prediction.summary.json")
        gcs_hook = GoogleCloudStorageHook()
        summary = json.loads(gcs_hook.download(bucket, summary))
        return validate_fn(summary)

    evaluate_validation = PythonOperator(
        task_id=(task_prefix + "-validation"),
        python_callable=apply_validate_fn,
        templates_dict={"prediction_path": prediction_path},
        dag=dag)
    evaluate_validation.set_upstream(evaluate_summary)

    return evaluate_prediction, evaluate_summary, evaluate_validation
