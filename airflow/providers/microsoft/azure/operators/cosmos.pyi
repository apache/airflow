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
#
from typing import Optional

from airflow.models import BaseOperator

class AzureCosmosInsertDocumentOperator(BaseOperator):
    """
    A stub file to suppress MyPy issues due to not supplying
    mandatory parameters to the operator
    """

    def __init__(
        self,
        *,
        database_name: Optional[str] = None,
        collection_name: Optional[str] = None,
        document: Optional[dict] = None,
        azure_cosmos_conn_id: str = 'azure_cosmos_default',
        **kwargs,
    ) -> None: ...
