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
from typing import Any, Dict

from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class WebHdfsSensor(BaseSensorOperator):
    """
    Waits for a file or folder to land in HDFS
    """
    template_fields = ('filepath',)

    @apply_defaults
    def __init__(self,
                 filepath: str,
                 webhdfs_conn_id: str = 'webhdfs_default',
                 *args: Any,
                 **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.filepath = filepath
        self.webhdfs_conn_id = webhdfs_conn_id

    def poke(self, context: Dict[Any, Any]) -> bool:
        from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
        hook = WebHDFSHook(self.webhdfs_conn_id)
        self.log.info('Poking for file %s', self.filepath)
        return hook.check_for_path(hdfs_path=self.filepath)
