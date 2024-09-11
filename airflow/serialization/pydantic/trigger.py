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

import datetime
from typing import Any, Dict, Optional

from pydantic import BaseModel as BaseModelPydantic, ConfigDict

from airflow.utils import timezone


class TriggerPydantic(BaseModelPydantic):
    """Serializable representation of the Trigger ORM SqlAlchemyModel used by internal API."""

    # This is technically non-optional, however when we serialize it in from_object we do not have the ID
    id: Optional[int]
    classpath: str
    encrypted_kwargs: str
    created_date: datetime.datetime
    triggerer_id: Optional[int]

    model_config = ConfigDict(from_attributes=True)

    def __init__(self, **kwargs) -> None:
        from airflow.models.trigger import Trigger

        # Here we have to handle two ways the object can be created:
        # * when Pydantic recreates it from Trigger model, we need a default __init__ behavior
        # * when we create it in from_object - the kwargs will contain classpath, kwargs to create it and
        #   created_date
        if "kwargs" in kwargs:
            self.classpath = kwargs.pop("classpath")
            self.encrypted_kwargs = Trigger._encrypt_kwargs(kwargs.pop("kwargs"))
            self.created_date = kwargs.pop("created_date", timezone.utcnow())
        super().__init__(**kwargs)

    @property
    def kwargs(self) -> Dict[str, Any]:
        """Return the decrypted kwargs of the trigger."""
        from airflow.models import Trigger

        return Trigger._decrypt_kwargs(self.encrypted_kwargs)

    @kwargs.setter
    def kwargs(self, kwargs: Dict[str, Any]) -> None:
        """Set the encrypted kwargs of the trigger."""
        from airflow.models import Trigger

        self.encrypted_kwargs = Trigger._encrypt_kwargs(kwargs)
