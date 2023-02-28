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
"""This module contains Google Cloud Language operators."""
from __future__ import annotations

from typing import TYPE_CHECKING, Sequence, Tuple

from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.api_core.retry import Retry
from google.cloud.language_v1 import enums
from google.cloud.language_v1.types import Document
from google.protobuf.json_format import MessageToDict

from airflow.providers.google.cloud.hooks.natural_language import CloudNaturalLanguageHook
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


MetaData = Sequence[Tuple[str, str]]


class CloudNaturalLanguageAnalyzeEntitiesOperator(GoogleCloudBaseOperator):
    """
    Finds named entities in the text along with entity types,
    salience, mentions for each entity, and other properties.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudNaturalLanguageAnalyzeEntitiesOperator`

    :param document: Input document.
        If a dict is provided, it must be of the same form as the protobuf message Document
    :param encoding_type: The encoding type used by the API to calculate offsets.
    :param retry: A retry object used to retry requests. If None is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        retry is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    # [START natural_language_analyze_entities_template_fields]
    template_fields: Sequence[str] = (
        "document",
        "gcp_conn_id",
        "impersonation_chain",
    )
    # [END natural_language_analyze_entities_template_fields]

    def __init__(
        self,
        *,
        document: dict | Document,
        encoding_type: enums.EncodingType | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: MetaData = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.document = document
        self.encoding_type = encoding_type
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudNaturalLanguageHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

        self.log.info("Start analyzing entities")
        response = hook.analyze_entities(
            document=self.document, retry=self.retry, timeout=self.timeout, metadata=self.metadata
        )
        self.log.info("Finished analyzing entities")

        return MessageToDict(response)


class CloudNaturalLanguageAnalyzeEntitySentimentOperator(GoogleCloudBaseOperator):
    """
    Finds entities, similar to AnalyzeEntities in the text and analyzes sentiment associated with each
    entity and its mentions.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudNaturalLanguageAnalyzeEntitySentimentOperator`

    :param document: Input document.
        If a dict is provided, it must be of the same form as the protobuf message Document
    :param encoding_type: The encoding type used by the API to calculate offsets.
    :param retry: A retry object used to retry requests. If None is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        retry is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).

    """

    # [START natural_language_analyze_entity_sentiment_template_fields]
    template_fields: Sequence[str] = (
        "document",
        "gcp_conn_id",
        "impersonation_chain",
    )
    # [END natural_language_analyze_entity_sentiment_template_fields]

    def __init__(
        self,
        *,
        document: dict | Document,
        encoding_type: enums.EncodingType | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: MetaData = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.document = document
        self.encoding_type = encoding_type
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudNaturalLanguageHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

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


class CloudNaturalLanguageAnalyzeSentimentOperator(GoogleCloudBaseOperator):
    """
    Analyzes the sentiment of the provided text.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudNaturalLanguageAnalyzeSentimentOperator`

    :param document: Input document.
        If a dict is provided, it must be of the same form as the protobuf message Document
    :param encoding_type: The encoding type used by the API to calculate offsets.
    :param retry: A retry object used to retry requests. If None is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        retry is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).

    """

    # [START natural_language_analyze_sentiment_template_fields]
    template_fields: Sequence[str] = (
        "document",
        "gcp_conn_id",
        "impersonation_chain",
    )
    # [END natural_language_analyze_sentiment_template_fields]

    def __init__(
        self,
        *,
        document: dict | Document,
        encoding_type: enums.EncodingType | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: MetaData = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.document = document
        self.encoding_type = encoding_type
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudNaturalLanguageHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

        self.log.info("Start sentiment analyze")
        response = hook.analyze_sentiment(
            document=self.document, retry=self.retry, timeout=self.timeout, metadata=self.metadata
        )
        self.log.info("Finished sentiment analyze")

        return MessageToDict(response)


class CloudNaturalLanguageClassifyTextOperator(GoogleCloudBaseOperator):
    """
    Classifies a document into categories.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudNaturalLanguageClassifyTextOperator`

    :param document: Input document.
        If a dict is provided, it must be of the same form as the protobuf message Document
    :param retry: A retry object used to retry requests. If None is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        retry is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    # [START natural_language_classify_text_template_fields]
    template_fields: Sequence[str] = (
        "document",
        "gcp_conn_id",
        "impersonation_chain",
    )
    # [END natural_language_classify_text_template_fields]

    def __init__(
        self,
        *,
        document: dict | Document,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: MetaData = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.document = document
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudNaturalLanguageHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

        self.log.info("Start text classify")
        response = hook.classify_text(
            document=self.document, retry=self.retry, timeout=self.timeout, metadata=self.metadata
        )
        self.log.info("Finished text classify")

        return MessageToDict(response)
