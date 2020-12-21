:mod:`airflow.providers.google.cloud.operators.speech_to_text`
==============================================================

.. py:module:: airflow.providers.google.cloud.operators.speech_to_text

.. autoapi-nested-parse::

   This module contains a Google Speech to Text operator.



Module Contents
---------------

.. py:class:: CloudSpeechToTextRecognizeSpeechOperator(*, audio: RecognitionAudio, config: RecognitionConfig, project_id: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', retry: Optional[Retry] = None, timeout: Optional[float] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Recognizes speech from audio file and returns it as text.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudSpeechToTextRecognizeSpeechOperator`

   :param config: information to the recognizer that specifies how to process the request. See more:
       https://googleapis.github.io/google-cloud-python/latest/speech/gapic/v1/types.html#google.cloud.speech_v1.types.RecognitionConfig
   :type config: dict or google.cloud.speech_v1.types.RecognitionConfig
   :param audio: audio data to be recognized. See more:
       https://googleapis.github.io/google-cloud-python/latest/speech/gapic/v1/types.html#google.cloud.speech_v1.types.RecognitionAudio
   :type audio: dict or google.cloud.speech_v1.types.RecognitionAudio
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
      :annotation: = ['audio', 'config', 'project_id', 'gcp_conn_id', 'timeout', 'impersonation_chain']

      

   
   .. method:: _validate_inputs(self)



   
   .. method:: execute(self, context)




