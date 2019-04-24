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

from airflow import AirflowException
from airflow.contrib.hooks.gcp_speech_to_text_hook import GCPSpeechToTextHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class GcpSpeechToTextRecognizeSpeechOperator(BaseOperator):
    """
    Recognizes speech from audio file and returns it as text.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GcpSpeechToTextRecognizeSpeechOperator`

    :param config: information to the recognizer that specifies how to process the request. See more:
        https://googleapis.github.io/google-cloud-python/latest/speech/gapic/v1/types.html#google.cloud.speech_v1.types.RecognitionConfig
    :type config: dict or google.cloud.speech_v1.types.RecognitionConfig
    :param audio: audio data to be recognized. See more:
        https://googleapis.github.io/google-cloud-python/latest/speech/gapic/v1/types.html#google.cloud.speech_v1.types.RecognitionAudio
    :type audio: dict or google.cloud.speech_v1.types.RecognitionAudio
    :param project_id: Optional, Google Cloud Platform Project ID where the Compute
        Engine Instance exists.  If set to None or missing, the default project_id from the GCP connection is
        used.
    :type project_id: str
    :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud
        Platform. Defaults to 'google_cloud_default'.
    :type gcp_conn_id: str
    :param retry: (Optional) A retry object used to retry requests. If None is specified,
            requests will not be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request to complete.
        Note that if retry is specified, the timeout applies to each individual attempt.
    :type timeout: float
    """

    # [START gcp_speech_to_text_synthesize_template_fields]
    template_fields = ("audio", "config", "project_id", "gcp_conn_id", "timeout")
    # [END gcp_speech_to_text_synthesize_template_fields]

    @apply_defaults
    def __init__(
        self,
        audio,
        config,
        project_id=None,
        gcp_conn_id="google_cloud_default",
        retry=None,
        timeout=None,
        *args,
        **kwargs
    ):
        self.audio = audio
        self.config = config
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.retry = retry
        self.timeout = timeout
        self._validate_inputs()
        super().__init__(*args, **kwargs)

    def _validate_inputs(self):
        if self.audio == "":
            raise AirflowException("The required parameter 'audio' is empty")
        if self.config == "":
            raise AirflowException("The required parameter 'config' is empty")

    def execute(self, context):
        _hook = GCPSpeechToTextHook(gcp_conn_id=self.gcp_conn_id)
        return _hook.recognize_speech(
            config=self.config, audio=self.audio, retry=self.retry, timeout=self.timeout
        )
