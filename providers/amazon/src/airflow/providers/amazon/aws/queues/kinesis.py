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

from typing import TYPE_CHECKING

from airflow.providers.amazon.aws.triggers.kinesis import KinesisTrigger
from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

try:
    from airflow.providers.common.messaging.providers.base_provider import BaseMessageQueueProvider
except ImportError:
    raise AirflowOptionalProviderFeatureException(
        "This feature requires the 'common.messaging' provider to be installed in version >= 2.0.0."
    )

if TYPE_CHECKING:
    from airflow.triggers.base import BaseEventTrigger


class KinesisMessageQueueProvider(BaseMessageQueueProvider):
    """
    Configuration for Amazon Kinesis Data Streams integration with common-messaging.

    Dispatches ``scheme="kinesis"`` to
    :class:`~airflow.providers.amazon.aws.triggers.kinesis.KinesisTrigger`, which also defines
    the accepted parameters.
    """

    scheme = "kinesis"

    def trigger_class(self) -> type[BaseEventTrigger]:
        return KinesisTrigger
