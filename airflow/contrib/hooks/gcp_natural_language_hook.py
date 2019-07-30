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

from google.cloud.language_v1 import LanguageServiceClient


from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook


# noinspection PyAbstractClass
class CloudNaturalLanguageHook(GoogleCloudBaseHook):
    """
    Hook for Google Cloud Natural Language Service.

    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    """

    _conn = None

    def __init__(self, gcp_conn_id="google_cloud_default", delegate_to=None):
        super(CloudNaturalLanguageHook, self).__init__(gcp_conn_id, delegate_to)

    def get_conn(self):
        """
        Retrieves connection to Cloud Natural Language service.

        :return: Cloud Natural Language service object
        :rtype: google.cloud.language_v1.LanguageServiceClient
        """
        if not self._conn:
            self._conn = LanguageServiceClient(credentials=self._get_credentials())
        return self._conn

    @GoogleCloudBaseHook.catch_http_exception
    @GoogleCloudBaseHook.quota_retry()
    def analyze_entities(self, document, encoding_type=None, retry=None, timeout=None, metadata=None):
        """
        Finds named entities in the text along with entity types,
        salience, mentions for each entity, and other properties.

        :param document: Input document.
            If a dict is provided, it must be of the same form as the protobuf message Document
        :type document: dict or class google.cloud.language_v1.types.Document
        :param encoding_type: The encoding type used by the API to calculate offsets.
        :type encoding_type: google.cloud.language_v1.types.EncodingType
        :param retry: A retry object used to retry requests. If None is specified, requests will not be
            retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            retry is specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: sequence[tuple[str, str]]]
        :rtype: google.cloud.language_v1.types.AnalyzeEntitiesResponse
        """
        client = self.get_conn()

        return client.analyze_entities(
            document=document, encoding_type=encoding_type, retry=retry, timeout=timeout, metadata=metadata
        )

    @GoogleCloudBaseHook.catch_http_exception
    @GoogleCloudBaseHook.quota_retry()
    def analyze_entity_sentiment(self, document, encoding_type=None, retry=None, timeout=None, metadata=None):
        """
        Finds entities, similar to AnalyzeEntities in the text and analyzes sentiment associated with each
        entity and its mentions.

        :param document: Input document.
            If a dict is provided, it must be of the same form as the protobuf message Document
        :type document: dict or class google.cloud.language_v1.types.Document
        :param encoding_type: The encoding type used by the API to calculate offsets.
        :type encoding_type: google.cloud.language_v1.types.EncodingType
        :param retry: A retry object used to retry requests. If None is specified, requests will not be
            retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            retry is specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: sequence[tuple[str, str]]]
        :rtype: google.cloud.language_v1.types.AnalyzeEntitiesResponse
        """
        client = self.get_conn()

        return client.analyze_entity_sentiment(
            document=document, encoding_type=encoding_type, retry=retry, timeout=timeout, metadata=metadata
        )

    @GoogleCloudBaseHook.catch_http_exception
    @GoogleCloudBaseHook.quota_retry()
    def analyze_sentiment(self, document, encoding_type=None, retry=None, timeout=None, metadata=None):
        """
        Analyzes the sentiment of the provided text.

        :param document: Input document.
            If a dict is provided, it must be of the same form as the protobuf message Document
        :type document: dict or class google.cloud.language_v1.types.Document
        :param encoding_type: The encoding type used by the API to calculate offsets.
        :type encoding_type: google.cloud.language_v1.types.EncodingType
        :param retry: A retry object used to retry requests. If None is specified, requests will not be
            retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            retry is specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: sequence[tuple[str, str]]]
        :rtype: google.cloud.language_v1.types.AnalyzeEntitiesResponse
        """
        client = self.get_conn()

        return client.analyze_sentiment(
            document=document, encoding_type=encoding_type, retry=retry, timeout=timeout, metadata=metadata
        )

    @GoogleCloudBaseHook.catch_http_exception
    @GoogleCloudBaseHook.quota_retry()
    def analyze_syntax(self, document, encoding_type=None, retry=None, timeout=None, metadata=None):
        """
        Analyzes the syntax of the text and provides sentence boundaries and tokenization along with part
        of speech tags, dependency trees, and other properties.

        :param document: Input document.
            If a dict is provided, it must be of the same form as the protobuf message Document
        :type document: dict or class google.cloud.language_v1.types.Document#
        :param encoding_type: The encoding type used by the API to calculate offsets.
        :type encoding_type: google.cloud.language_v1.types.EncodingType
        :param retry: A retry object used to retry requests. If None is specified, requests will not be
            retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            retry is specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: sequence[tuple[str, str]]]
        :rtype: google.cloud.language_v1.types.AnalyzeSyntaxResponse
        """
        client = self.get_conn()

        return client.analyze_syntax(
            document=document, encoding_type=encoding_type, retry=retry, timeout=timeout, metadata=metadata
        )

    @GoogleCloudBaseHook.catch_http_exception
    @GoogleCloudBaseHook.quota_retry()
    def annotate_text(self, document, features, encoding_type=None, retry=None, timeout=None, metadata=None):
        """
        A convenience method that provides all the features that analyzeSentiment,
        analyzeEntities, and analyzeSyntax provide in one call.

        :param document: Input document.
            If a dict is provided, it must be of the same form as the protobuf message Document
        :type document: dict or google.cloud.language_v1.types.Document
        :param features: The enabled features.
            If a dict is provided, it must be of the same form as the protobuf message Features
        :type features: dict or google.cloud.language_v1.enums.Features
        :param encoding_type: The encoding type used by the API to calculate offsets.
        :type encoding_type: google.cloud.language_v1.types.EncodingType
        :param retry: A retry object used to retry requests. If None is specified, requests will not be
            retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            retry is specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: sequence[tuple[str, str]]]
        :rtype: google.cloud.language_v1.types.AnnotateTextResponse
        """
        client = self.get_conn()

        return client.annotate_text(
            document=document,
            features=features,
            encoding_type=encoding_type,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleCloudBaseHook.catch_http_exception
    @GoogleCloudBaseHook.quota_retry()
    def classify_text(self, document, retry=None, timeout=None, metadata=None):
        """
        Classifies a document into categories.

        :param document: Input document.
            If a dict is provided, it must be of the same form as the protobuf message Document
        :type document: dict or class google.cloud.language_v1.types.Document
        :param retry: A retry object used to retry requests. If None is specified, requests will not be
            retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            retry is specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: sequence[tuple[str, str]]]
        :rtype: google.cloud.language_v1.types.AnalyzeEntitiesResponse
        """
        client = self.get_conn()

        return client.classify_text(document=document, retry=retry, timeout=timeout, metadata=metadata)
