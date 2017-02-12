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

from airflow.dataflow import Dataflow
import os
import json
try:
    # Python 2
    import cPickle as pickle
except:
    # Python 3
    import pickle

class FileDataflow(Dataflow):
    def __init__(
            self,
            upstream_task,
            index=None,
            path='/tmp/airflow/dataflow',
            serializer='pickle'):
        """
        This Dataflow uses the local filesystem to store data. It can only be
        used in environments where all Airflow processes have read and write
        access to the filesystem.

        path: files will be written in subdirectories of this path
        serializer: data will be serialized by this method
        """
        serializers = ('pickle', 'json')
        if serializer not in serializers:
            raise ValueError(
                'Unsupported serializer: "{}". Provide one of: {}'.format(
                    serializer, serializers))
        self.serializer = serializer
        self.path = path

        super(FileDataflow, self).__init__(
            upstream_task=upstream_task,
            index=index)

    def serialize(self, data, context):
        fpath = os.path.join(self.path, self.key(context))
        os.makedirs(os.path.dirname(fpath), exist_ok=True)

        if self.serializer == 'pickle':
            with open(fpath, mode='wb') as f:
                pickle.dump(data, f)
        elif self.serializer == 'json':
            with open(fpath, mode='w') as f:
                json.dump(data, f)

        return fpath

    def deserialize(self, data, context):
        if self.serializer == 'pickle':
            with open(data, mode='rb') as f:
                data = pickle.load(f)
        elif self.serializer == 'json':
            with open(data, mode='r') as f:
                data = json.load(f)
        return data
