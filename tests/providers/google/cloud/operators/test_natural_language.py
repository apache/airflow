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
import unittest

from google.cloud.language_v1.proto.language_service_pb2 import (
    AnalyzeEntitiesResponse, AnalyzeEntitySentimentResponse, AnalyzeSentimentResponse, ClassifyTextResponse,
    Document,
)

from airflow.providers.google.cloud.operators.natural_language import (
    CloudLanguageAnalyzeEntitiesOperator, CloudLanguageAnalyzeEntitySentimentOperator,
    CloudLanguageAnalyzeSentimentOperator, CloudLanguageClassifyTextOperator,
)
from tests.compat import patch

DOCUMENT = Document(
    content="Airflow is a platform to programmatically author, schedule and monitor workflows."
)

CLASSIFY_TEXT_RRESPONSE = ClassifyTextResponse()
ANALYZE_ENTITIES_RESPONSE = AnalyzeEntitiesResponse()
ANALYZE_ENTITY_SENTIMENT_RESPONSE = AnalyzeEntitySentimentResponse()
ANALYZE_SENTIMENT_RESPONSE = AnalyzeSentimentResponse()

ENCODING_TYPE = "UTF32"


class TestCloudLanguageAnalyzeEntitiesOperator(unittest.TestCase):
    @patch("airflow.providers.google.cloud.operators.natural_language.CloudNaturalLanguageHook")
    def test_minimal_green_path(self, hook_mock):
        hook_mock.return_value.analyze_entities.return_value = ANALYZE_ENTITIES_RESPONSE
        op = CloudLanguageAnalyzeEntitiesOperator(task_id="task-id", document=DOCUMENT)
        resp = op.execute({})
        self.assertEqual(resp, {})


class TestCloudLanguageAnalyzeEntitySentimentOperator(unittest.TestCase):
    @patch("airflow.providers.google.cloud.operators.natural_language.CloudNaturalLanguageHook")
    def test_minimal_green_path(self, hook_mock):
        hook_mock.return_value.analyze_entity_sentiment.return_value = ANALYZE_ENTITY_SENTIMENT_RESPONSE
        op = CloudLanguageAnalyzeEntitySentimentOperator(task_id="task-id", document=DOCUMENT)
        resp = op.execute({})
        self.assertEqual(resp, {})


class TestCloudLanguageAnalyzeSentimentOperator(unittest.TestCase):
    @patch("airflow.providers.google.cloud.operators.natural_language.CloudNaturalLanguageHook")
    def test_minimal_green_path(self, hook_mock):
        hook_mock.return_value.analyze_sentiment.return_value = ANALYZE_SENTIMENT_RESPONSE
        op = CloudLanguageAnalyzeSentimentOperator(task_id="task-id", document=DOCUMENT)
        resp = op.execute({})
        self.assertEqual(resp, {})


class TestCloudLanguageClassifyTextOperator(unittest.TestCase):
    @patch("airflow.providers.google.cloud.operators.natural_language.CloudNaturalLanguageHook")
    def test_minimal_green_path(self, hook_mock):
        hook_mock.return_value.classify_text.return_value = CLASSIFY_TEXT_RRESPONSE
        op = CloudLanguageClassifyTextOperator(task_id="task-id", document=DOCUMENT)
        resp = op.execute({})
        self.assertEqual(resp, {})
