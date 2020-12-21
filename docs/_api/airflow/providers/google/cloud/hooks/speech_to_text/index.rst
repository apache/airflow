:mod:`airflow.providers.google.cloud.hooks.speech_to_text`
==========================================================

.. py:module:: airflow.providers.google.cloud.hooks.speech_to_text

.. autoapi-nested-parse::

   This module contains a Google Cloud Speech Hook.



Module Contents
---------------

.. py:class:: CloudSpeechToTextHook(gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None)

   Bases: :class:`airflow.providers.google.common.hooks.base_google.GoogleBaseHook`

   Hook for Google Cloud Speech API.

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

      Retrieves connection to Cloud Speech.

      :return: Google Cloud Speech client object.
      :rtype: google.cloud.speech_v1.SpeechClient



   
   .. method:: recognize_speech(self, config: Union[Dict, RecognitionConfig], audio: Union[Dict, RecognitionAudio], retry: Optional[Retry] = None, timeout: Optional[float] = None)

      Recognizes audio input

      :param config: information to the recognizer that specifies how to process the request.
          https://googleapis.github.io/google-cloud-python/latest/speech/gapic/v1/types.html#google.cloud.speech_v1.types.RecognitionConfig
      :type config: dict or google.cloud.speech_v1.types.RecognitionConfig
      :param audio: audio data to be recognized
          https://googleapis.github.io/google-cloud-python/latest/speech/gapic/v1/types.html#google.cloud.speech_v1.types.RecognitionAudio
      :type audio: dict or google.cloud.speech_v1.types.RecognitionAudio
      :param retry: (Optional) A retry object used to retry requests. If None is specified,
          requests will not be retried.
      :type retry: google.api_core.retry.Retry
      :param timeout: (Optional) The amount of time, in seconds, to wait for the request to complete.
          Note that if retry is specified, the timeout applies to each individual attempt.
      :type timeout: float




