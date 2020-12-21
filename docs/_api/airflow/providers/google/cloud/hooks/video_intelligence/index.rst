:mod:`airflow.providers.google.cloud.hooks.video_intelligence`
==============================================================

.. py:module:: airflow.providers.google.cloud.hooks.video_intelligence

.. autoapi-nested-parse::

   This module contains a Google Cloud Video Intelligence Hook.



Module Contents
---------------

.. py:class:: CloudVideoIntelligenceHook(gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None)

   Bases: :class:`airflow.providers.google.common.hooks.base_google.GoogleBaseHook`

   Hook for Google Cloud Video Intelligence APIs.

   All the methods in the hook where project_id is used must be called with
   keyword arguments rather than positional.

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

      Returns Gcp Video Intelligence Service client

      :rtype: google.cloud.videointelligence_v1.VideoIntelligenceServiceClient



   
   .. method:: annotate_video(self, input_uri: Optional[str] = None, input_content: Optional[bytes] = None, features: Optional[List[VideoIntelligenceServiceClient.enums.Feature]] = None, video_context: Union[Dict, VideoContext] = None, output_uri: Optional[str] = None, location: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Performs video annotation.

      :param input_uri: Input video location. Currently, only Google Cloud Storage URIs are supported,
          which must be specified in the following format: ``gs://bucket-id/object-id``.
      :type input_uri: str
      :param input_content: The video data bytes.
          If unset, the input video(s) should be specified via ``input_uri``.
          If set, ``input_uri`` should be unset.
      :type input_content: bytes
      :param features: Requested video annotation features.
      :type features: list[google.cloud.videointelligence_v1.VideoIntelligenceServiceClient.enums.Feature]
      :param output_uri: Optional, location where the output (in JSON format) should be stored. Currently,
          only Google Cloud Storage URIs are supported, which must be specified in the following format:
          ``gs://bucket-id/object-id``.
      :type output_uri: str
      :param video_context: Optional, Additional video context and/or feature-specific parameters.
      :type video_context: dict or google.cloud.videointelligence_v1.types.VideoContext
      :param location: Optional, cloud region where annotation should take place. Supported cloud regions:
          us-east1, us-west1, europe-west1, asia-east1.
          If no region is specified, a region will be determined based on video file location.
      :type location: str
      :param retry: Retry object used to determine when/if to retry requests.
          If None is specified, requests will not be retried.
      :type retry: google.api_core.retry.Retry
      :param timeout: Optional, The amount of time, in seconds, to wait for the request to complete.
          Note that if retry is specified, the timeout applies to each individual attempt.
      :type timeout: float
      :param metadata: Optional, Additional metadata that is provided to the method.
      :type metadata: seq[tuple[str, str]]




