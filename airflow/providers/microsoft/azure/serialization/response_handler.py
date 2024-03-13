from typing import Optional, Dict, Any, Callable

from kiota_abstractions.response_handler import ResponseHandler, NativeResponseType
from kiota_abstractions.serialization import ParsableFactory  # noqa: TC002


class CallableResponseHandler(ResponseHandler):
    def __init__(
        self,
        callable_function: Callable[
            [NativeResponseType, Optional[Dict[str, Optional[ParsableFactory]]]], Any
        ],
    ):
        self.callable_function = callable_function

    async def handle_response_async(
        self,
        response: NativeResponseType,
        error_map: Optional[Dict[str, Optional[ParsableFactory]]],
    ) -> Any:
        return self.callable_function(response, error_map)
