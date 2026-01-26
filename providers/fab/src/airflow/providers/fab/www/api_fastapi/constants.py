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
"""Constants for FastAPI migration from connexion."""
from __future__ import annotations

from airflow.utils.docs import get_docs_url

doc_link = get_docs_url("stable-rest-api-ref.html")

EXCEPTIONS_LINK_MAP = {
    400: f"{doc_link}#section/Errors/BadRequest",
    401: f"{doc_link}#section/Errors/Unauthenticated",
    403: f"{doc_link}#section/Errors/PermissionDenied",
    404: f"{doc_link}#section/Errors/NotFound",
    405: f"{doc_link}#section/Errors/MethodNotAllowed",
    409: f"{doc_link}#section/Errors/AlreadyExists",
    500: f"{doc_link}#section/Errors/Unknown",
}
