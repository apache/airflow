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

import re
from collections.abc import Sequence, AsyncIterator
from functools import cached_property
from typing import Any, NoReturn

from airflow.exceptions import AirflowException, AirflowFailException
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.providers.common.msgq.hooks.msg_queue import MsgQueueHook

class MessageQueueTrigger(BaseTrigger):
    """
    Defer until a message for a particular topic is published on the message queue
    
    When the message arrives, 
    - get the data for the particular message and post it to XCom
    - Alternatively, trigger a registered function, need more input here

    Resume waiting for the next message
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init(**kwargs)

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """
        Need to insert the processing here based on the decision above
        """
        
    
