:mod:`airflow.providers.google.cloud.hooks.natural_language`
============================================================

.. py:module:: airflow.providers.google.cloud.hooks.natural_language

.. autoapi-nested-parse::

   This module contains a Google Cloud Natural Language Hook.



Module Contents
---------------

.. py:class:: CloudNaturalLanguageHook(gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None)

   Bases: :class:`airflow.providers.google.common.hooks.base_google.GoogleBaseHook`

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

   
   .. method:: get_conn(self)

      Retrieves connection to Cloud Natural Language service.

      :return: Cloud Natural Language service object
      :rtype: google.cloud.language_v1.LanguageServiceClient



   
   .. method:: analyze_entities(self, document: Union[dict, Document], encoding_type: Optional[enums.EncodingType] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

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



   
   .. method:: analyze_entity_sentiment(self, document: Union[dict, Document], encoding_type: Optional[enums.EncodingType] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

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



   
   .. method:: analyze_sentiment(self, document: Union[dict, Document], encoding_type: Optional[enums.EncodingType] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

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



   
   .. method:: analyze_syntax(self, document: Union[dict, Document], encoding_type: Optional[enums.EncodingType] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

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



   
   .. method:: annotate_text(self, document: Union[dict, Document], features: Union[dict, AnnotateTextRequest.Features], encoding_type: enums.EncodingType = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

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



   
   .. method:: classify_text(self, document: Union[dict, Document], retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

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




