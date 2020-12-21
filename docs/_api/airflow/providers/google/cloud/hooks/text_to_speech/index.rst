:mod:`airflow.providers.google.cloud.hooks.text_to_speech`
==========================================================

.. py:module:: airflow.providers.google.cloud.hooks.text_to_speech

.. autoapi-nested-parse::

   This module contains a Google Cloud Text to Speech Hook.



Module Contents
---------------

.. py:class:: CloudTextToSpeechHook(gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None)

   Bases: :class:`airflow.providers.google.common.hooks.base_google.GoogleBaseHook`

   Hook for Google Cloud Text to Speech API.

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

      Retrieves connection to Cloud Text to Speech.

      :return: Google Cloud Text to Speech client object.
      :rtype: google.cloud.texttospeech_v1.TextToSpeechClient



   
   .. method:: synthesize_speech(self, input_data: Union[Dict, SynthesisInput], voice: Union[Dict, VoiceSelectionParams], audio_config: Union[Dict, AudioConfig], retry: Optional[Retry] = None, timeout: Optional[float] = None)

      Synthesizes text input

      :param input_data: text input to be synthesized. See more:
          https://googleapis.github.io/google-cloud-python/latest/texttospeech/gapic/v1/types.html#google.cloud.texttospeech_v1.types.SynthesisInput
      :type input_data: dict or google.cloud.texttospeech_v1.types.SynthesisInput
      :param voice: configuration of voice to be used in synthesis. See more:
          https://googleapis.github.io/google-cloud-python/latest/texttospeech/gapic/v1/types.html#google.cloud.texttospeech_v1.types.VoiceSelectionParams
      :type voice: dict or google.cloud.texttospeech_v1.types.VoiceSelectionParams
      :param audio_config: configuration of the synthesized audio. See more:
          https://googleapis.github.io/google-cloud-python/latest/texttospeech/gapic/v1/types.html#google.cloud.texttospeech_v1.types.AudioConfig
      :type audio_config: dict or google.cloud.texttospeech_v1.types.AudioConfig
      :param retry: (Optional) A retry object used to retry requests. If None is specified,
              requests will not be retried.
      :type retry: google.api_core.retry.Retry
      :param timeout: (Optional) The amount of time, in seconds, to wait for the request to complete.
          Note that if retry is specified, the timeout applies to each individual attempt.
      :type timeout: float
      :return: SynthesizeSpeechResponse See more:
          https://googleapis.github.io/google-cloud-python/latest/texttospeech/gapic/v1/types.html#google.cloud.texttospeech_v1.types.SynthesizeSpeechResponse
      :rtype: object




