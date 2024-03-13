import json
import locale
from base64 import b64encode
from contextlib import suppress
from datetime import datetime
from json import JSONDecodeError
from typing import Optional, Any
from uuid import UUID

import pendulum


class ResponseSerializer:
    def __init__(self, encoding: Optional[str] = None):
        self.encoding = encoding or locale.getpreferredencoding()

    def serialize(self, response) -> Optional[str]:
        def convert(value) -> Optional[str]:
            if value is not None:
                if isinstance(value, UUID):
                    return str(value)
                if isinstance(value, datetime):
                    return value.isoformat()
                if isinstance(value, pendulum.DateTime):
                    return value.to_iso8601_string()  # Adjust the format as needed
                raise TypeError(
                    f"Object of type {type(value)} is not JSON serializable!"
                )
            return None

        if response is not None:
            if isinstance(response, bytes):
                return b64encode(response).decode(self.encoding)
            with suppress(JSONDecodeError):
                return json.dumps(response, default=convert)
            return response
        return None

    def deserialize(self, response) -> Any:
        if isinstance(response, str):
            with suppress(JSONDecodeError):
                response = json.loads(response)
        return response
