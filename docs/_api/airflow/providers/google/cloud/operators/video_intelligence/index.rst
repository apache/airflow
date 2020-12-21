:mod:`airflow.providers.google.cloud.operators.video_intelligence`
==================================================================

.. py:module:: airflow.providers.google.cloud.operators.video_intelligence

.. autoapi-nested-parse::

   This module contains Google Cloud Vision operators.



Module Contents
---------------

.. py:class:: CloudVideoIntelligenceDetectVideoLabelsOperator(*, input_uri: str, input_content: Optional[bytes] = None, output_uri: Optional[str] = None, video_context: Union[Dict, VideoContext] = None, location: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Performs video annotation, annotating video labels.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudVideoIntelligenceDetectVideoLabelsOperator`.

   :param input_uri: Input video location. Currently, only Google Cloud Storage URIs are supported,
       which must be specified in the following format: ``gs://bucket-id/object-id``.
   :type input_uri: str
   :param input_content: The video data bytes.
       If unset, the input video(s) should be specified via ``input_uri``.
       If set, ``input_uri`` should be unset.
   :type input_content: bytes
   :param output_uri: Optional, location where the output (in JSON format) should be stored. Currently, only
       Google Cloud Storage URIs are supported, which must be specified in the following format:
       ``gs://bucket-id/object-id``.
   :type output_uri: str
   :param video_context: Optional, Additional video context and/or feature-specific parameters.
   :type video_context: dict or google.cloud.videointelligence_v1.types.VideoContext
   :param location: Optional, cloud region where annotation should take place. Supported cloud regions:
       us-east1, us-west1, europe-west1, asia-east1. If no region is specified, a region will be determined
       based on video file location.
   :type location: str
   :param retry: Retry object used to determine when/if to retry requests.
       If None is specified, requests will not be retried.
   :type retry: google.api_core.retry.Retry
   :param timeout: Optional, The amount of time, in seconds, to wait for the request to complete.
       Note that if retry is specified, the timeout applies to each individual attempt.
   :type timeout: float
   :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud.
       Defaults to ``google_cloud_default``.
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
      :annotation: = ['input_uri', 'output_uri', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudVideoIntelligenceDetectVideoExplicitContentOperator(*, input_uri: str, output_uri: Optional[str] = None, input_content: Optional[bytes] = None, video_context: Union[Dict, VideoContext] = None, location: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Performs video annotation, annotating explicit content.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudVideoIntelligenceDetectVideoExplicitContentOperator`

   :param input_uri: Input video location. Currently, only Google Cloud Storage URIs are supported,
       which must be specified in the following format: ``gs://bucket-id/object-id``.
   :type input_uri: str
   :param input_content: The video data bytes.
       If unset, the input video(s) should be specified via ``input_uri``.
       If set, ``input_uri`` should be unset.
   :type input_content: bytes
   :param output_uri: Optional, location where the output (in JSON format) should be stored. Currently, only
       Google Cloud Storage URIs are supported, which must be specified in the following format:
       ``gs://bucket-id/object-id``.
   :type output_uri: str
   :param video_context: Optional, Additional video context and/or feature-specific parameters.
   :type video_context: dict or google.cloud.videointelligence_v1.types.VideoContext
   :param location: Optional, cloud region where annotation should take place. Supported cloud regions:
       us-east1, us-west1, europe-west1, asia-east1. If no region is specified, a region will be determined
       based on video file location.
   :type location: str
   :param retry: Retry object used to determine when/if to retry requests.
       If None is specified, requests will not be retried.
   :type retry: google.api_core.retry.Retry
   :param timeout: Optional, The amount of time, in seconds, to wait for the request to complete.
       Note that if retry is specified, the timeout applies to each individual attempt.
   :type timeout: float
   :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud
       Defaults to ``google_cloud_default``.
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
      :annotation: = ['input_uri', 'output_uri', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudVideoIntelligenceDetectVideoShotsOperator(*, input_uri: str, output_uri: Optional[str] = None, input_content: Optional[bytes] = None, video_context: Union[Dict, VideoContext] = None, location: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Performs video annotation, annotating video shots.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudVideoIntelligenceDetectVideoShotsOperator`

   :param input_uri: Input video location. Currently, only Google Cloud Storage URIs are supported,
       which must be specified in the following format: ``gs://bucket-id/object-id``.
   :type input_uri: str
   :param input_content: The video data bytes.
       If unset, the input video(s) should be specified via ``input_uri``.
       If set, ``input_uri`` should be unset.
   :type input_content: bytes
   :param output_uri: Optional, location where the output (in JSON format) should be stored. Currently, only
       Google Cloud Storage URIs are supported, which must be specified in the following format:
       ``gs://bucket-id/object-id``.
   :type output_uri: str
   :param video_context: Optional, Additional video context and/or feature-specific parameters.
   :type video_context: dict or google.cloud.videointelligence_v1.types.VideoContext
   :param location: Optional, cloud region where annotation should take place. Supported cloud regions:
       us-east1, us-west1, europe-west1, asia-east1. If no region is specified, a region will be determined
       based on video file location.
   :type location: str
   :param retry: Retry object used to determine when/if to retry requests.
       If None is specified, requests will not be retried.
   :type retry: google.api_core.retry.Retry
   :param timeout: Optional, The amount of time, in seconds, to wait for the request to complete.
       Note that if retry is specified, the timeout applies to each individual attempt.
   :type timeout: float
   :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud.
       Defaults to ``google_cloud_default``.
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
      :annotation: = ['input_uri', 'output_uri', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




