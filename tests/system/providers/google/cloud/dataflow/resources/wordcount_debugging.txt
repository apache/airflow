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
An example that verifies the counts and includes best practices.

On top of the basic concepts in the wordcount example, this workflow introduces
logging to Cloud Logging, and using assertions in a Dataflow pipeline.
To execute this pipeline locally, specify a local output file or output prefix
on GCS::

  --output [YOUR_LOCAL_FILE | gs://YOUR_OUTPUT_PREFIX]

To execute this pipeline using the Google Cloud Dataflow service, specify
pipeline configuration::

  --project YOUR_PROJECT_ID
  --staging_location gs://YOUR_STAGING_DIRECTORY
  --temp_location gs://YOUR_TEMP_DIRECTORY
  --region GCE_REGION
  --job_name YOUR_JOB_NAME
  --runner DataflowRunner

and an output prefix on GCS.
"""

# pytype: skip-file

from __future__ import annotations

import argparse
import logging
import re

import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.testing.util import assert_that, equal_to


class FilterTextFn(beam.DoFn):
    """A DoFn that filters for a specific key based on a regular expression."""

    def __init__(self, pattern):
        # TODO(BEAM-6158): Revert the workaround once we can pickle super() on py3.
        # super(FilterTextFn, self).__init__()
        beam.DoFn.__init__(self)
        self.pattern = pattern
        # A custom metric can track values in your pipeline as it runs. Those
        # values will be available in the monitoring system of the runner used
        # to run the pipeline. These metrics below track the number of
        # matched and unmatched words.
        self.matched_words = Metrics.counter(self.__class__, "matched_words")
        self.umatched_words = Metrics.counter(self.__class__, "umatched_words")

    def process(self, element):
        word, _ = element
        if re.match(self.pattern, word):
            # Log at INFO level each element we match. When executing this pipeline
            # using the Dataflow service, these log lines will appear in the Cloud
            # Logging UI.
            logging.info("Matched %s", word)
            self.matched_words.inc()
            yield element
        else:
            # Log at the "DEBUG" level each element that is not matched. Different log
            # levels can be used to control the verbosity of logging providing an
            # effective mechanism to filter less important information.
            # Note currently only "INFO" and higher level logs are emitted to the
            # Cloud Logger. This log message will not be visible in the Cloud Logger.
            logging.debug("Did not match %s", word)
            self.umatched_words.inc()


class CountWords(beam.PTransform):
    """
    A transform to count the occurrences of each word.

    A PTransform that converts a PCollection containing lines of text into a
    PCollection of (word, count) tuples.
    """

    def expand(self, pcoll):
        def count_ones(word_ones):
            (word, ones) = word_ones
            return (word, sum(ones))

        return (
            pcoll
            | "split" >> (beam.FlatMap(lambda x: re.findall(r"[A-Za-z\']+", x)).with_output_types(str))
            | "pair_with_one" >> beam.Map(lambda x: (x, 1))
            | "group" >> beam.GroupByKey()
            | "count" >> beam.Map(count_ones)
        )


def run(argv=None, save_main_session=True):
    """Run the debugging wordcount pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        dest="input",
        default="gs://dataflow-samples/shakespeare/kinglear.txt",
        help="Input file to process.",
    )
    parser.add_argument("--output", dest="output", required=True, help="Output file to write results to.")
    known_args, pipeline_args = parser.parse_known_args(argv)
    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    with beam.Pipeline(options=pipeline_options) as p:

        # Read the text file[pattern] into a PCollection, count the occurrences of
        # each word and filter by a list of words.
        filtered_words = (
            p
            | "read" >> ReadFromText(known_args.input)
            | CountWords()
            | "FilterText" >> beam.ParDo(FilterTextFn("Flourish|stomach"))
        )

        # assert_that is a convenient PTransform that checks a PCollection has an
        # expected value. Asserts are best used in unit tests with small data sets
        # but is demonstrated here as a teaching tool.
        #
        # Note assert_that does not provide any output and that successful
        # completion of the Pipeline implies that the expectations were  met. Learn
        # more at https://cloud.google.com/dataflow/pipelines/testing-your-pipeline
        # on how to best test your pipeline.
        assert_that(filtered_words, equal_to([("Flourish", 3), ("stomach", 1)]))

        # Format the counts into a PCollection of strings and write the output using
        # a "Write" transform that has side effects.
        # pylint: disable=unused-variable
        def format_result(word_count):
            (word, count) = word_count
            return f"{word}: {count}"

        _ = filtered_words | "format" >> beam.Map(format_result) | "write" >> WriteToText(known_args.output)


if __name__ == "__main__":
    # Cloud Logging would contain only logging.INFO and higher level logs logged
    # by the root logger. All log statements emitted by the root logger will be
    # visible in the Cloud Logging UI. Learn more at
    # https://cloud.google.com/logging about the Cloud Logging UI.
    #
    # You can set the default logging level to a different level when running
    # locally.
    logging.getLogger().setLevel(logging.INFO)
    run()
