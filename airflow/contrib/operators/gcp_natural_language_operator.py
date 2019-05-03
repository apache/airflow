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
from google.protobuf.json_format import MessageToDict

from airflow.contrib.hooks.gcp_natural_language_hook import CloudNaturalLanguageHook
from airflow.models import BaseOperator


class CloudLanguageAnalyzeEntitiesOperator(BaseOperator):
    """
    Finds named entities in the text along with entity types,
    salience, mentions for each entity, and other properties.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudLanguageAnalyzeEntitiesOperator`

    :param document: Input document.
        If a dict is provided, it must be of the same form as the protobuf message Document
    :type document: dict or google.cloud.language_v1.types.Document
    :param encoding_type: The encoding type used by the API to calculate offsets.
    :type encoding_type: google.cloud.language_v1.types.EncodingType
    :param retry: A retry object used to retry requests. If None is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        retry is specified, the timeout applies to each individual attempt.
    :type timeout: float
    :param metadata: Additional metadata that is provided to the method.
    :type metadata: seq[tuple[str, str]]]
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :type gcp_conn_id: str
    """

    # [START natural_language_analyze_entities_template_fields]
    template_fields = ("document", "gcp_conn_id")
    # [END natural_language_analyze_entities_template_fields]

    def __init__(
        self,
        document,
        encoding_type=None,
        retry=None,
        timeout=None,
        metadata=None,
        gcp_conn_id="google_cloud_default",
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
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


class CloudLanguageAnalyzeEntitySentimentOperator(BaseOperator):
    """
    Finds entities, similar to AnalyzeEntities in the text and analyzes sentiment associated with each
    entity and its mentions.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudLanguageAnalyzeEntitySentimentOperator`

    :param document: Input document.
        If a dict is provided, it must be of the same form as the protobuf message Document
    :type document: dict or google.cloud.language_v1.types.Document
    :param encoding_type: The encoding type used by the API to calculate offsets.
    :type encoding_type: google.cloud.language_v1.types.EncodingType
    :param retry: A retry object used to retry requests. If None is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        retry is specified, the timeout applies to each individual attempt.
    :type timeout: float
    :param metadata: Additional metadata that is provided to the method.
    :type metadata: seq[tuple[str, str]]]
    :rtype: google.cloud.language_v1.types.AnalyzeEntitiesResponse
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :type gcp_conn_id: str
    """

    # [START natural_language_analyze_entity_sentiment_template_fields]
    template_fields = ("document", "gcp_conn_id")
    # [END natural_language_analyze_entity_sentiment_template_fields]

    def __init__(
        self,
        document,
        encoding_type=None,
        retry=None,
        timeout=None,
        metadata=None,
        gcp_conn_id="google_cloud_default",
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
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


class CloudLanguageAnalyzeSentimentOperator(BaseOperator):
    """
    Analyzes the sentiment of the provided text.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudLanguageAnalyzeSentimentOperator`

    :param document: Input document.
        If a dict is provided, it must be of the same form as the protobuf message Document
    :type document: dict or google.cloud.language_v1.types.Document
    :param encoding_type: The encoding type used by the API to calculate offsets.
    :type encoding_type: google.cloud.language_v1.types.EncodingType
    :param retry: A retry object used to retry requests. If None is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        retry is specified, the timeout applies to each individual attempt.
    :type timeout: float
    :param metadata: Additional metadata that is provided to the method.
    :type metadata: sequence[tuple[str, str]]]
    :rtype: google.cloud.language_v1.types.AnalyzeEntitiesResponse
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :type gcp_conn_id: str
    """

    # [START natural_language_analyze_sentiment_template_fields]
    template_fields = ("document", "gcp_conn_id")
    # [END natural_language_analyze_sentiment_template_fields]

    def __init__(
        self,
        document,
        encoding_type=None,
        retry=None,
        timeout=None,
        metadata=None,
        gcp_conn_id="google_cloud_default",
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
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


class CloudLanguageClassifyTextOperator(BaseOperator):
    """
    Classifies a document into categories.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudLanguageClassifyTextOperator`

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

    def __init__(
        self,
        document,
        retry=None,
        timeout=None,
        metadata=None,
        gcp_conn_id="google_cloud_default",
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
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
