# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from tempfile import NamedTemporaryFile
import subprocess

from airflow.exceptions import AirflowException
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class S3FileTransformOperator(BaseOperator):
    """
    Copies data from a source S3 location to a temporary location on the
    local filesystem. Runs a transformation on this file as specified by
    the transformation script and uploads the output to a destination S3
    location.

    The locations of the source and the destination files in the local
    filesystem is provided as an first and second arguments to the
    transformation script. The transformation script is expected to read the
    data from source , transform it and write the output to the local
    destination file. The operator then takes over control and uploads the
    local destination file to S3.

    :param source_s3_key: The key to be retrieved from S3
    :type source_s3_key: str
    :param source_s3_conn_id: source s3 connection
    :type source_s3_conn_id: str
    :param dest_s3_key: The key to be written from S3
    :type dest_s3_key: str
    :param dest_s3_conn_id: destination s3 connection
    :type dest_s3_conn_id: str
    :param replace: Replace dest S3 key if it already exists
    :type replace: bool
    :param transform_script: location of the executable transformation script
    :type transform_script: str
    """

    template_fields = ('source_s3_key', 'dest_s3_key')
    template_ext = ()
    ui_color = '#f9c915'

    @apply_defaults
    def __init__(
            self,
            source_s3_key,
            dest_s3_key,
            transform_script,
            source_s3_conn_id='s3_default',
            dest_s3_conn_id='s3_default',
            replace=False,
            *args, **kwargs):
        super(S3FileTransformOperator, self).__init__(*args, **kwargs)
        self.source_s3_key = source_s3_key
        self.source_s3_conn_id = source_s3_conn_id
        self.dest_s3_key = dest_s3_key
        self.dest_s3_conn_id = dest_s3_conn_id
        self.replace = replace
        self.transform_script = transform_script

    def execute(self, context):
        source_s3 = S3Hook(s3_conn_id=self.source_s3_conn_id)
        dest_s3 = S3Hook(s3_conn_id=self.dest_s3_conn_id)
        self.log.info("Downloading source S3 file %s", self.source_s3_key)
        if not source_s3.check_for_key(self.source_s3_key):
            raise AirflowException("The source key {0} does not exist".format(self.source_s3_key))
        source_s3_key_object = source_s3.get_key(self.source_s3_key)
        with NamedTemporaryFile("w") as f_source, NamedTemporaryFile("w") as f_dest:
            self.log.info(
                "Dumping S3 file %s contents to local file %s",
                self.source_s3_key, f_source.name
            )
            source_s3_key_object.get_contents_to_file(f_source)
            f_source.flush()
            source_s3.connection.close()
            transform_script_process = subprocess.Popen(
                [self.transform_script, f_source.name, f_dest.name],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            (transform_script_stdoutdata, transform_script_stderrdata) = transform_script_process.communicate()
            self.log.info("Transform script stdout %s", transform_script_stdoutdata)
            if transform_script_process.returncode > 0:
                raise AirflowException("Transform script failed %s", transform_script_stderrdata)
            else:
                self.log.info(
                    "Transform script successful. Output temporarily located at %s",
                    f_dest.name
                )
            self.log.info("Uploading transformed file to S3")
            f_dest.flush()
            dest_s3.load_file(
                filename=f_dest.name,
                key=self.dest_s3_key,
                replace=self.replace
            )
            self.log.info("Upload successful")
            dest_s3.connection.close()
