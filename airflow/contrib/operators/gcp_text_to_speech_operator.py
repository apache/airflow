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

from tempfile import NamedTemporaryFile

from airflow import AirflowException
from airflow.contrib.hooks.gcp_text_to_speech_hook import GCPTextToSpeechHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class GcpTextToSpeechSynthesizeOperator(BaseOperator):
    """
    Synthesizes text to speech and stores it in Google Cloud Storage

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GcpTextToSpeechSynthesizeOperator`

    :param input_: text input to be synthesized. See more:
        https://googleapis.github.io/google-cloud-python/latest/texttospeech/gapic/v1/types.html#google.cloud.texttospeech_v1.types.SynthesisInput
    :type input_: dict or google.cloud.texttospeech_v1.types.SynthesisInput
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
    :param project_id: Optional, Google Cloud Platform Project ID where the Compute
        Engine Instance exists.  If set to None or missing, the default project_id from the GCP connection is
        used.
    :type project_id: str
    :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud
        Platform. Defaults to 'google_cloud_default'.
    :type gcp_conn_id: str
    """

    # [START gcp_text_to_speech_synthesize_template_fields]
    template_fields = ("project_id", "gcp_conn_id", "target_bucket_name", "target_filename")
    # [END gcp_text_to_speech_synthesize_template_fields]

    @apply_defaults
    def __init__(
        self,
        input_,
        voice,
        audio_config,
        target_bucket_name,
        target_filename,
        project_id=None,
        gcp_conn_id="google_cloud_default",
        *args,
        **kwargs
    ):
        self.input_ = input_
        self.voice = voice
        self.audio_config = audio_config
        self.target_bucket_name = target_bucket_name
        self.target_filename = target_filename
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self._validate_inputs()
        super(GcpTextToSpeechSynthesizeOperator, self).__init__(*args, **kwargs)

    def _validate_inputs(self):
        if self.project_id == "":
            raise AirflowException("The required parameter 'project_id' is empty")
        if self.input_ == "":
            raise AirflowException("The required parameter 'input_' is empty")
        if self.voice == "":
            raise AirflowException("The required parameter 'voice' is empty")
        if self.audio_config == "":
            raise AirflowException("The required parameter 'audio_config' is empty")
        if self.target_bucket_name == "":
            raise AirflowException("The required parameter 'target_bucket_name' is empty")
        if self.target_filename == "":
            raise AirflowException("The required parameter 'target_filename' is empty")

    def execute(self, context):

        gcp_text_to_speech_hook = GCPTextToSpeechHook(gcp_conn_id=self.gcp_conn_id)
        result = gcp_text_to_speech_hook.synthesize_speech(
            input_=self.input_, voice=self.voice, audio_config=self.audio_config
        )
        with NamedTemporaryFile() as temp_file:
            temp_file.write(result.audio_content)
            cloud_storage_hook = GoogleCloudStorageHook(google_cloud_storage_conn_id=self.gcp_conn_id)
            cloud_storage_hook.upload(
                bucket=self.target_bucket_name, object=self.target_filename, filename=temp_file.name
            )
