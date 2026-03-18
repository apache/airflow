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
"""
A template called by DataFlowPythonOperator to summarize BatchPrediction.

It accepts a user function to calculate the metric(s) per instance in
the prediction results, then aggregates to output as a summary.

It accepts the following arguments:

- ``--prediction_path``:
  The GCS folder that contains BatchPrediction results, containing
  ``prediction.results-NNNNN-of-NNNNN`` files in the json format.
  Output will be also stored in this folder, as 'prediction.summary.json'.
- ``--metric_fn_encoded``:
  An encoded function that calculates and returns a tuple of metric(s)
  for a given instance (as a dictionary). It should be encoded
  via ``base64.b64encode(dill.dumps(fn, recurse=True))``.
- ``--metric_keys``:
  A comma-separated key(s) of the aggregated metric(s) in the summary
  output. The order and the size of the keys must match to the output
  of metric_fn.
  The summary will have an additional key, 'count', to represent the
  total number of instances, so the keys shouldn't include 'count'.


Usage example:

.. code-block: python

    from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator


    def get_metric_fn():
        import math  # all imports must be outside of the function to be passed.
        def metric_fn(inst):
            label = float(inst["input_label"])
            classes = float(inst["classes"])
            prediction = float(inst["scores"][1])
            log_loss = math.log(1 + math.exp(
                -(label * 2 - 1) * math.log(prediction / (1 - prediction))))
            squared_err = (classes-label)**2
            return (log_loss, squared_err)
        return metric_fn
    metric_fn_encoded = base64.b64encode(dill.dumps(get_metric_fn(), recurse=True))
    DataflowCreatePythonJobOperator(
        task_id="summary-prediction",
        py_options=["-m"],
        py_file="airflow.providers.google.cloud.utils.mlengine_prediction_summary",
        options={
            "prediction_path": prediction_path,
            "metric_fn_encoded": metric_fn_encoded,
            "metric_keys": "log_loss,mse"
        },
        dataflow_default_options={
            "project": "xxx", "region": "us-east1",
            "staging_location": "gs://yy", "temp_location": "gs://zz",
        }
    ) >> dag

When the input file is like the following::

    {"inputs": "1,x,y,z", "classes": 1, "scores": [0.1, 0.9]}
    {"inputs": "0,o,m,g", "classes": 0, "scores": [0.7, 0.3]}
    {"inputs": "1,o,m,w", "classes": 0, "scores": [0.6, 0.4]}
    {"inputs": "1,b,r,b", "classes": 1, "scores": [0.2, 0.8]}

The output file will be::

    {"log_loss": 0.43890510565304547, "count": 4, "mse": 0.25}

To test outside of the dag:

.. code-block:: python

    subprocess.check_call(
        [
            "python",
            "-m",
            "airflow.providers.google.cloud.utils.mlengine_prediction_summary",
            "--prediction_path=gs://...",
            "--metric_fn_encoded=" + metric_fn_encoded,
            "--metric_keys=log_loss,mse",
            "--runner=DataflowRunner",
            "--staging_location=gs://...",
            "--temp_location=gs://...",
        ]
    )

.. spelling:word-list::

    pcoll
"""

from __future__ import annotations

import argparse
import base64
import json
import logging
import os

import apache_beam as beam
import dill
from apache_beam.coders.coders import Coder


class JsonCoder(Coder):
    """JSON encoder/decoder."""

    @staticmethod
    def encode(x):
        """JSON encoder."""
        return json.dumps(x).encode()

    @staticmethod
    def decode(x):
        """JSON decoder."""
        return json.loads(x)


@beam.ptransform_fn
def MakeSummary(pcoll, metric_fn, metric_keys):
    """Summary PTransform used in Dataflow."""
    return (
        pcoll
        | "ApplyMetricFnPerInstance" >> beam.Map(metric_fn)
        | "PairWith1" >> beam.Map(lambda tup: (*tup, 1))
        | "SumTuple" >> beam.CombineGlobally(beam.combiners.TupleCombineFn(*([sum] * (len(metric_keys) + 1))))
        | "AverageAndMakeDict"
        >> beam.Map(
            lambda tup: dict(
                [(name, tup[i] / tup[-1]) for i, name in enumerate(metric_keys)] + [("count", tup[-1])]
            )
        )
    )


def run(argv=None):
    """Obtain prediction summary."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--prediction_path",
        required=True,
        help=(
            "The GCS folder that contains BatchPrediction results, containing "
            "prediction.results-NNNNN-of-NNNNN files in the json format. "
            "Output will be also stored in this folder, as a file"
            "'prediction.summary.json'."
        ),
    )
    parser.add_argument(
        "--metric_fn_encoded",
        required=True,
        help=(
            "An encoded function that calculates and returns a tuple of "
            "metric(s) for a given instance (as a dictionary). It should be "
            "encoded via base64.b64encode(dill.dumps(fn, recurse=True))."
        ),
    )
    parser.add_argument(
        "--metric_keys",
        required=True,
        help=(
            "A comma-separated keys of the aggregated metric(s) in the summary "
            "output. The order and the size of the keys must match to the "
            "output of metric_fn. The summary will have an additional key, "
            "'count', to represent the total number of instances, so this flag "
            "shouldn't include 'count'."
        ),
    )
    known_args, pipeline_args = parser.parse_known_args(argv)

    metric_fn = dill.loads(base64.b64decode(known_args.metric_fn_encoded))
    if not callable(metric_fn):
        raise ValueError("--metric_fn_encoded must be an encoded callable.")
    metric_keys = known_args.metric_keys.split(",")

    with beam.Pipeline(options=beam.pipeline.PipelineOptions(pipeline_args)) as pipe:
        prediction_result_pattern = os.path.join(known_args.prediction_path, "prediction.results-*-of-*")
        prediction_summary_path = os.path.join(known_args.prediction_path, "prediction.summary.json")
        # This is apache-beam ptransform's convention
        _ = (
            pipe
            | "ReadPredictionResult" >> beam.io.ReadFromText(prediction_result_pattern, coder=JsonCoder())
            | "Summary" >> MakeSummary(metric_fn, metric_keys)
            | "Write"
            >> beam.io.WriteToText(
                prediction_summary_path,
                shard_name_template="",  # without trailing -NNNNN-of-NNNNN.
                coder=JsonCoder(),
            )
        )


if __name__ == "__main__":
    # Dataflow does not print anything on the screen by default. Good practice says to configure the logger
    # to be able to track the progress. This code is run in a separate process, so it's safe.
    logging.getLogger().setLevel(logging.INFO)
    run()
