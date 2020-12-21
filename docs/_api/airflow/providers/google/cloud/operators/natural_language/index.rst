:mod:`airflow.providers.google.cloud.operators.natural_language`
================================================================

.. py:module:: airflow.providers.google.cloud.operators.natural_language

.. autoapi-nested-parse::

   This module contains Google Cloud Language operators.



Module Contents
---------------

.. data:: MetaData
   

   

.. py:class:: CloudNaturalLanguageAnalyzeEntitiesOperator(*, document: Union[dict, Document], encoding_type: Optional[enums.EncodingType] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[MetaData] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

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
   :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
   :type gcp_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['document', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudNaturalLanguageAnalyzeEntitySentimentOperator(*, document: Union[dict, Document], encoding_type: Optional[enums.EncodingType] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[MetaData] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

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
   :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
   :type gcp_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   :rtype: google.cloud.language_v1.types.AnalyzeEntitiesResponse

   .. attribute:: template_fields
      :annotation: = ['document', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudNaturalLanguageAnalyzeSentimentOperator(*, document: Union[dict, Document], encoding_type: Optional[enums.EncodingType] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[MetaData] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

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
   :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
   :type gcp_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   :rtype: google.cloud.language_v1.types.AnalyzeEntitiesResponse

   .. attribute:: template_fields
      :annotation: = ['document', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudNaturalLanguageClassifyTextOperator(*, document: Union[dict, Document], retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[MetaData] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

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
   :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
   :type gcp_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['document', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




