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
This module contains a Google Cloud Natural Language Hook.
"""
from typing import Dict, Optional, Sequence, Tuple, Union

from google.api_core.retry import Retry
from google.cloud.language_v1 import LanguageServiceClient, enums
from google.cloud.language_v1.types import (
    AnalyzeEntitiesResponse,
    AnalyzeEntitySentimentResponse,
    AnalyzeSentimentResponse,
    AnalyzeSyntaxResponse,
    AnnotateTextRequest,
    AnnotateTextResponse,
    ClassifyTextResponse,
    Document,
)

from airflow.providers.google.common.hooks.base_google import GoogleBaseHook


class CloudNaturalLanguageHook(GoogleBaseHook):
    """
    Hook for Google Cloud Natural Language Service.

    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account.
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            delegate_to=delegate_to,
            impersonation_chain=impersonation_chain,
        )
        self._conn = None

    def get_conn(self) -> LanguageServiceClient:
        """
        Retrieves connection to Cloud Natural Language service.

        :return: Cloud Natural Language service object
        :rtype: google.cloud.language_v1.LanguageServiceClient
        """
        if not self._conn:
            self._conn = LanguageServiceClient(
                credentials=self._get_credentials(), client_info=self.client_info
            )
        return self._conn

    @GoogleBaseHook.quota_retry()
    def analyze_entities(
        self,
        document: Union[Dict, Document],
        encoding_type: Optional[enums.EncodingType] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> AnalyzeEntitiesResponse:
        """
        Finds named entities in the text along with entity types,
        salience, mentions for each entity, and other properties.

        :param document: Input document.
            If a dict is provided, it must be of the same form as the protobuf message Document
        :type document: dict or google.cloud.language_v1.types.Document
        :param encoding_type: The encoding type used by the API to calculate offsets.
        :type encoding_type: google.cloud.language_v1.enums.EncodingType
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

    @GoogleBaseHook.quota_retry()
    def analyze_entity_sentiment(
        self,
        document: Union[Dict, Document],
        encoding_type: Optional[enums.EncodingType] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> AnalyzeEntitySentimentResponse:
        """
        Finds entities, similar to AnalyzeEntities in the text and analyzes sentiment associated with each
        entity and its mentions.

        :param document: Input document.
            If a dict is provided, it must be of the same form as the protobuf message Document
        :type document: dict or google.cloud.language_v1.types.Document
        :param encoding_type: The encoding type used by the API to calculate offsets.
        :type encoding_type: google.cloud.language_v1.enums.EncodingType
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

    @GoogleBaseHook.quota_retry()
    def analyze_sentiment(
        self,
        document: Union[Dict, Document],
        encoding_type: Optional[enums.EncodingType] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> AnalyzeSentimentResponse:
        """
        Analyzes the sentiment of the provided text.

        :param document: Input document.
            If a dict is provided, it must be of the same form as the protobuf message Document
        :type document: dict or google.cloud.language_v1.types.Document
        :param encoding_type: The encoding type used by the API to calculate offsets.
        :type encoding_type: google.cloud.language_v1.enums.EncodingType
        :param retry: A retry object used to retry requests. If None is specified, requests will not be
            retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            retry is specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: sequence[tuple[str, str]]]
        :rtype: google.cloud.language_v1.types.AnalyzeSentimentResponse
        """
        client = self.get_conn()

        return client.analyze_sentiment(
            document=document, encoding_type=encoding_type, retry=retry, timeout=timeout, metadata=metadata
        )

    @GoogleBaseHook.quota_retry()
    def analyze_syntax(
        self,
        document: Union[Dict, Document],
        encoding_type: Optional[enums.EncodingType] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> AnalyzeSyntaxResponse:
        """
        Analyzes the syntax of the text and provides sentence boundaries and tokenization along with part
        of speech tags, dependency trees, and other properties.

        :param document: Input document.
            If a dict is provided, it must be of the same form as the protobuf message Document
        :type document: dict or google.cloud.language_v1.types.Document
        :param encoding_type: The encoding type used by the API to calculate offsets.
        :type encoding_type: google.cloud.language_v1.enums.EncodingType
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

    @GoogleBaseHook.quota_retry()
    def annotate_text(
        self,
        document: Union[Dict, Document],
        features: Union[Dict, AnnotateTextRequest.Features],
        encoding_type: enums.EncodingType = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> AnnotateTextResponse:
        """
        A convenience method that provides all the features that analyzeSentiment,
        analyzeEntities, and analyzeSyntax provide in one call.

        :param document: Input document.
            If a dict is provided, it must be of the same form as the protobuf message Document
        :type document: dict or google.cloud.language_v1.types.Document
        :param features: The enabled features.
            If a dict is provided, it must be of the same form as the protobuf message Features
        :type features: dict or google.cloud.language_v1.types.AnnotateTextRequest.Features
        :param encoding_type: The encoding type used by the API to calculate offsets.
        :type encoding_type: google.cloud.language_v1.enums.EncodingType
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

    @GoogleBaseHook.quota_retry()
    def classify_text(
        self,
        document: Union[Dict, Document],
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> ClassifyTextResponse:
        """
        Classifies a document into categories.

        :param document: Input document.
            If a dict is provided, it must be of the same form as the protobuf message Document
        :type document: dict or google.cloud.language_v1.types.Document
        :param retry: A retry object used to retry requests. If None is specified, requests will not be
            retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            retry is specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: sequence[tuple[str, str]]]
        :rtype: google.cloud.language_v1.types.ClassifyTextResponse
        """
        client = self.get_conn()

        return client.classify_text(document=document, retry=retry, timeout=timeout, metadata=metadata)
