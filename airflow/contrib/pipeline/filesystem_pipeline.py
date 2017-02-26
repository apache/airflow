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

from airflow.contrib.pipeline import Pipeline
import os


class FileSystemPipeline(Pipeline):

    def __init__(
            self,
            downstream_task,
            downstream_key,
            upstream_task,
            upstream_key=None,
            path_prefix='/tmp/airflow/',
            extension='.air',
            serialize=None,
            deserialize=None):
        """
        FileSystemPipeline serializes data to the local filesystem. It can ONLY
        be used if all Airflow processes have access to the same filesystem.

        Files will be written to the provided path_prefix.
        """
        super(FileSystemPipeline, self).__init__(
            downstream_task=downstream_task,
            downstream_key=downstream_key,
            upstream_task=upstream_task,
            upstream_key=upstream_key,
            serialize=serialize,
            deserialize=deserialize)
        self.path_prefix = path_prefix
        self.extension = extension

    def _serialize(self, data, context):
        fpath = os.path.join(
            self.path_prefix,
            self.generate_unique_id(context, extension=self.extension))
        os.makedirs(os.path.dirname(fpath), exist_ok=True)
        with open(fpath, mode='w') as f:
            f.write(self.serialize(data, context))
        return fpath

    def _deserialize(self, fpath, context):
        self._fpath = fpath
        with open(fpath, mode='r') as f:
            data = self.deserialize(f.read(), context)
        return data

    def clean_up(self, context):
        if hasattr(self, '_fpath') and self._fpath.startswith(self.path_prefix):
            os.remove(self._fpath)
            del self._fpath
