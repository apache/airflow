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
This module contains Google Cloud Language operators.
"""
from typing import Optional, Sequence, Tuple, Union

from google.api_core.retry import Retry
from google.cloud.language_v1 import enums
from google.cloud.language_v1.types import Document
from google.protobuf.json_format import MessageToDict

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.natural_language import CloudNaturalLanguageHook
from airflow.utils.decorators import apply_defaults

MetaData = Sequence[Tuple[str, str]]


class CloudNaturalLanguageAnalyzeEntitiesOperator(BaseOperator):
    """
    Finds named entities in the text along with entity types,
    salience, mentions for each entity, and other properties.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudNaturalLanguageAnalyzeEntitiesOperator`

    :param document: Input document.
        If a dict is provided, it must be of the same form as the protobuf message Document
    :type document: dict or google.cloud.language_v1.types.Document
    :param encoding_type: The encoding type used by the API to calculate offsets.
    :type encoding_type: google.cloud.language_v1.enums.EncodingType
    :param retry: A retry object used to retry requests. If None is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        retry is specified, the timeout applies to each individual attempt.
    :type timeout: float
    :param metadata: Additional metadata that is provided to the method.
    :type metadata: Sequence[Tuple[str, str]]
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :type gcp_conn_id: str
    """
    # [START natural_language_analyze_entities_template_fields]
    template_fields = ("document", "gcp_conn_id")
    # [END natural_language_analyze_entities_template_fields]

    @apply_defaults
    def __init__(
        self, *,
        document: Union[dict, Document],
        encoding_type: Optional[enums.EncodingType] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[MetaData] = None,
        gcp_conn_id: str = "google_cloud_default",
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.document = document
        self.encoding_type = encoding_type
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context):
        hook = CloudNaturalLanguageHook(gcp_conn_id=self.gcp_conn_id)

        self.log.info("Start analyzing entities")
        response = hook.analyze_entities(
            document=self.document, retry=self.retry, timeout=self.timeout, metadata=self.metadata
        )
        self.log.info("Finished analyzing entities")

        return MessageToDict(response)


class CloudNaturalLanguageAnalyzeEntitySentimentOperator(BaseOperator):
    """
    Finds entities, similar to AnalyzeEntities in the text and analyzes sentiment associated with each
    entity and its mentions.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudNaturalLanguageAnalyzeEntitySentimentOperator`

    :param document: Input document.
        If a dict is provided, it must be of the same form as the protobuf message Document
    :type document: dict or google.cloud.language_v1.types.Document
    :param encoding_type: The encoding type used by the API to calculate offsets.
    :type encoding_type: google.cloud.language_v1.enums.EncodingType
    :param retry: A retry object used to retry requests. If None is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        retry is specified, the timeout applies to each individual attempt.
    :type timeout: float
    :param metadata: Additional metadata that is provided to the method.
    :type metadata: Sequence[Tuple[str, str]]]
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :type gcp_conn_id: str
    :rtype: google.cloud.language_v1.types.AnalyzeEntitiesResponse
    """
    # [START natural_language_analyze_entity_sentiment_template_fields]
    template_fields = ("document", "gcp_conn_id")
    # [END natural_language_analyze_entity_sentiment_template_fields]

    @apply_defaults
    def __init__(
        self, *,
        document: Union[dict, Document],
        encoding_type: Optional[enums.EncodingType] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[MetaData] = None,
        gcp_conn_id: str = "google_cloud_default",
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.document = document
        self.encoding_type = encoding_type
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context):
        hook = CloudNaturalLanguageHook(gcp_conn_id=self.gcp_conn_id)

        self.log.info("Start entity sentiment analyze")
        response = hook.analyze_entity_sentiment(
            document=self.document,
            encoding_type=self.encoding_type,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        self.log.info("Finished entity sentiment analyze")

        return MessageToDict(response)


class CloudNaturalLanguageAnalyzeSentimentOperator(BaseOperator):
    """
    Analyzes the sentiment of the provided text.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudNaturalLanguageAnalyzeSentimentOperator`

    :param document: Input document.
        If a dict is provided, it must be of the same form as the protobuf message Document
    :type document: dict or google.cloud.language_v1.types.Document
    :param encoding_type: The encoding type used by the API to calculate offsets.
    :type encoding_type: google.cloud.language_v1.enums.EncodingType
    :param retry: A retry object used to retry requests. If None is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        retry is specified, the timeout applies to each individual attempt.
    :type timeout: float
    :param metadata: Additional metadata that is provided to the method.
    :type metadata: sequence[tuple[str, str]]]
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :type gcp_conn_id: str
    :rtype: google.cloud.language_v1.types.AnalyzeEntitiesResponse
    """
    # [START natural_language_analyze_sentiment_template_fields]
    template_fields = ("document", "gcp_conn_id")
    # [END natural_language_analyze_sentiment_template_fields]

    @apply_defaults
    def __init__(
        self, *,
        document: Union[dict, Document],
        encoding_type: Optional[enums.EncodingType] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[MetaData] = None,
        gcp_conn_id: str = "google_cloud_default",
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.document = document
        self.encoding_type = encoding_type
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context):
        hook = CloudNaturalLanguageHook(gcp_conn_id=self.gcp_conn_id)

        self.log.info("Start sentiment analyze")
        response = hook.analyze_sentiment(
            document=self.document, retry=self.retry, timeout=self.timeout, metadata=self.metadata
        )
        self.log.info("Finished sentiment analyze")

        return MessageToDict(response)


class CloudNaturalLanguageClassifyTextOperator(BaseOperator):
    """
    Classifies a document into categories.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudNaturalLanguageClassifyTextOperator`

    :param document: Input document.
        If a dict is provided, it must be of the same form as the protobuf message Document
    :type document: dict or google.cloud.language_v1.types.Document
    :param retry: A retry object used to retry requests. If None is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        retry is specified, the timeout applies to each individual attempt.
    :type timeout: float
    :param metadata: Additional metadata that is provided to the method.
    :type metadata: sequence[tuple[str, str]]]
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :type gcp_conn_id: str
    """
    # [START natural_language_classify_text_template_fields]
    template_fields = ("document", "gcp_conn_id")
    # [END natural_language_classify_text_template_fields]

    @apply_defaults
    def __init__(
        self, *,
        document: Union[dict, Document],
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[MetaData] = None,
        gcp_conn_id: str = "google_cloud_default",
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.document = document
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context):
        hook = CloudNaturalLanguageHook(gcp_conn_id=self.gcp_conn_id)

        self.log.info("Start text classify")
        response = hook.classify_text(
            document=self.document, retry=self.retry, timeout=self.timeout, metadata=self.metadata
        )
        self.log.info("Finished text classify")

        return MessageToDict(response)
