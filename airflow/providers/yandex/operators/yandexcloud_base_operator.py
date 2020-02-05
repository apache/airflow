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
#

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class YandexCloudBaseOperator(BaseOperator):
    """The base class for operators that poll on a Dataproc Operation."""
    @apply_defaults
    def __init__(self,
                 connection_id='yandexcloud_default',
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.connection_id = connection_id
        self.hook = None

    def execute(self, context):
        raise NotImplementedError()
