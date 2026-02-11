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

from abc import ABC, abstractmethod

from pydantic_ai import ModelSettings
from pydantic_ai.models import Model

from airflow.utils.log.logging_mixin import LoggingMixin


class ModelProvider(LoggingMixin, ABC):
    """
    Base class for model providers.
    """

    @property
    @abstractmethod
    def provider_name(self) -> str:
        """
        Returns the name of the provider.
        """
        raise NotImplementedError


    def get_model_settings(self, **kwargs) -> ModelSettings | None:
        ...

    @abstractmethod
    def build_model(self, model_name: str, api_key: str, **kwargs) -> Model:
        """
        Builds and returns a model instance based on the provided model name and parameters.
        """
        raise NotImplementedError

