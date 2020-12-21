:mod:`airflow.providers.google.cloud.operators.text_to_speech`
==============================================================

.. py:module:: airflow.providers.google.cloud.operators.text_to_speech

.. autoapi-nested-parse::

   This module contains a Google Text to Speech operator.



Module Contents
---------------

.. py:class:: CloudTextToSpeechSynthesizeOperator(*, input_data: Union[Dict, SynthesisInput], voice: Union[Dict, VoiceSelectionParams], audio_config: Union[Dict, AudioConfig], target_bucket_name: str, target_filename: str, project_id: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', retry: Optional[Retry] = None, timeout: Optional[float] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Synthesizes text to speech and stores it in Google Cloud Storage

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudTextToSpeechSynthesizeOperator`

   :param input_data: text input to be synthesized. See more:
       https://googleapis.github.io/google-cloud-python/latest/texttospeech/gapic/v1/types.html#google.cloud.texttospeech_v1.types.SynthesisInput
   :type input_data: dict or google.cloud.texttospeech_v1.types.SynthesisInput
   :param voice: configuration of voice to be used in synthesis. See more:
       https://googleapis.github.io/google-cloud-python/latest/texttospeech/gapic/v1/types.html#google.cloud.texttospeech_v1.types.VoiceSelectionParams
   :type voice: dict or google.cloud.texttospeech_v1.types.VoiceSelectionParams
   :param audio_config: configuration of the synthesized audio. See more:
       https://googleapis.github.io/google-cloud-python/latest/texttospeech/gapic/v1/types.html#google.cloud.texttospeech_v1.types.AudioConfig
   :type audio_config: dict or google.cloud.texttospeech_v1.types.AudioConfig
   :param target_bucket_name: name of the GCS bucket in which output file should be stored
   :type target_bucket_name: str
   :param target_filename: filename of the output file.
   :type target_filename: str
   :param project_id: Optional, Google Cloud Project ID where the Compute
       Engine Instance exists. If set to None or missing, the default project_id from the Google Cloud
       connection is used.
   :type project_id: str
   :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud.
       Defaults to 'google_cloud_default'.
   :type gcp_conn_id: str
   :param retry: (Optional) A retry object used to retry requests. If None is specified,
           requests will not be retried.
   :type retry: google.api_core.retry.Retry
   :param timeout: (Optional) The amount of time, in seconds, to wait for the request to complete.
       Note that if retry is specified, the timeout applies to each individual attempt.
   :type timeout: float
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
      :annotation: = ['input_data', 'voice', 'audio_config', 'project_id', 'gcp_conn_id', 'target_bucket_name', 'target_filename', 'impersonation_chain']

      

   
   .. method:: _validate_inputs(self)



   
   .. method:: execute(self, context)




