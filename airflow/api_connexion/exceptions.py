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
from typing import Dict, Optional

from connexion import ProblemException


class NotFound(ProblemException):
    """Raise when the object cannot be found"""

    def __init__(self, title='Object not found', detail=None):
        super().__init__(status=404, title=title, detail=detail)


class BadRequest(ProblemException):
    """Raise when the server processes a bad request"""

    def __init__(self, title='Bad request', detail=None):
        super().__init__(status=400, title=title, detail=detail)


class Unauthenticated(ProblemException):
    """Raise when the user is not authenticated"""

    def __init__(
        self, title: str = 'Unauthorized', detail: Optional[str] = None, headers: Optional[Dict] = None,
    ):
        super().__init__(status=401, title=title, detail=detail, headers=headers)


class PermissionDenied(ProblemException):
    """Raise when the user does not have the required permissions"""

    def __init__(self, title='Forbidden', detail=None):
        super().__init__(status=403, title=title, detail=detail)


class AlreadyExists(ProblemException):
    """Raise when the object already exists"""

    def __init__(self, title='Object already exists', detail=None):
        super().__init__(status=409, title=title, detail=detail)


class Unknown(ProblemException):
    """Returns a response body and status code for HTTP 500 exception"""

    def __init__(self, title='Unknown server error', detail=None):
        super().__init__(status=500, title=title, detail=detail)
