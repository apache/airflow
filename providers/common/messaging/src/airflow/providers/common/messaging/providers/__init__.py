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
from __future__ import annotations

import importlib

from airflow.providers_manager import ProvidersManager
from airflow.utils.deprecation_tools import add_deprecated_classes

providers_manager = ProvidersManager()
providers_manager.initialize_providers_queues()


def create_class_by_name(name: str):
    module_name, class_name = name.rsplit(".", 1)
    module = importlib.import_module(module_name)
    return getattr(module, class_name)


MESSAGE_QUEUE_PROVIDERS = [create_class_by_name(name)() for name in providers_manager.queue_class_names]

__deprecated_classes = {
    "sqs": {
        "SqsMessageQueueProvider": "airflow.providers.amazon.aws.queues.sqs.SqsMessageQueueProvider",
    },
}

add_deprecated_classes(__deprecated_classes, __name__)
