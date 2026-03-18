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

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from airflow.api_fastapi.execution_api.deps import DepContainer

router = APIRouter()


@router.get("")
def health() -> dict:
    return {"status": "healthy"}


@router.get("/ping")
async def ping(services=DepContainer):
    ok: list[str] = []
    failing: dict[str, str] = {}
    code = 200

    for svc in services.get_pings():
        try:
            await svc.aping()
            ok.append(svc.name)
        except Exception as e:
            failing[svc.name] = repr(e)
            code = 500

    return JSONResponse(content={"ok": ok, "failing": failing}, status_code=code)
