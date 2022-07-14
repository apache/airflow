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

from airflow.callbacks.callback_requests import CallbackRequest
from airflow.dag_processing.processor import DagFileProcessor
from airflow.internal_api.grpc import internal_api_pb2_grpc
from airflow.internal_api.grpc.internal_api_pb2 import FileProcessorRequest, FileProcessorResponse


class FileProcessorServiceServicer(internal_api_pb2_grpc.FileProcessorServiceServicer):
    """Provides methods that implement functionality of File Processor service."""

    def __init__(self, processor: DagFileProcessor):
        super().__init__()
        self.dag_file_processor = processor

    def processFile(self, request: FileProcessorRequest, context):
        dags_found, errors_found = self.dag_file_processor.process_file(
            file_path=request.path,
            callback_requests=CallbackRequest.get_callbacks_from_protobuf(request.callbacks),
            pickle_dags=request.pickle_dags,
        )
        return FileProcessorResponse(dagsFound=dags_found, errorsFound=errors_found)
