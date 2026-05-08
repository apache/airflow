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

"""
Informatica EDC Simulator.

A lightweight FastAPI application that mimics the Informatica Enterprise Data
Catalog (EDC) REST API.  It is intended for local development and Breeze-based
integration testing of the Airflow Informatica provider.

Implemented endpoints
---------------------
GET  /access
    Health-check — returns {"status": "ok"}.

GET  /access/2/catalog/data/search
    Simulates the EDC catalog search used by InformaticaEDCHook to resolve
    URIs.  Accepts the same query-string parameters as the real API and
    returns a hit whose ``id`` matches the ``fq`` filter when supplied.

GET  /access/2/catalog/data/objects/{object_id}
    Returns a minimal catalog object.  The object is created on first access
    and stored in the in-memory registry.

PATCH /access/1/catalog/data/objects
    Accepts the lineage-link payload sent by
    ``InformaticaEDCHook.create_lineage_link`` and records the relationship
    in memory.

GET  /lineage
    Development-only — returns all recorded lineage links.

DELETE /lineage
    Clears all stored lineage links and catalog objects.
"""

from __future__ import annotations

import re
import urllib.parse
import uuid
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

app = FastAPI(title="Informatica EDC Simulator", version="1.0.0")

# ---------------------------------------------------------------------------
# In-memory stores
# ---------------------------------------------------------------------------

_catalog: dict[str, dict[str, Any]] = {
    "TEST_PSTGRS://mydb/public/orders": {
        "id": "TEST_PSTGRS://mydb/public/orders",
        "name": "orders",
        "type": "com.infa.ldm.relational.Table",
        "core": {
            "name": "orders",
            "classType": "com.infa.ldm.relational.Table",
        },
    },
    "TEST_PSTGRS://mydb/public/customers": {
        "id": "TEST_PSTGRS://mydb/public/customers",
        "name": "customers",
        "type": "com.infa.ldm.relational.Table",
        "core": {
            "name": "customers",
            "classType": "com.infa.ldm.relational.Table",
        },
    },
    "TEST_PSTGRS://mydb/public/products": {
        "id": "TEST_PSTGRS://mydb/public/products",
        "name": "products",
        "type": "com.infa.ldm.relational.Table",
        "core": {
            "name": "products",
            "classType": "com.infa.ldm.relational.Table",
        },
    },
    "TEST_PSTGRS://mydb/public/order_summary": {
        "id": "TEST_PSTGRS://mydb/public/order_summary",
        "name": "order_summary",
        "type": "com.infa.ldm.relational.Table",
        "core": {
            "name": "order_summary",
            "classType": "com.infa.ldm.relational.Table",
        },
    },
    "TEST_PSTGRS://mydb/public/customer_ltv": {
        "id": "TEST_PSTGRS://mydb/public/customer_ltv",
        "name": "customer_ltv",
        "type": "com.infa.ldm.relational.Table",
        "core": {
            "name": "customer_ltv",
            "classType": "com.infa.ldm.relational.Table",
        },
    },
    "TEST_PSTGRS://mydb/public/customer_segment_snapshot": {
        "id": "TEST_PSTGRS://mydb/public/customer_segment_snapshot",
        "name": "customer_segment_snapshot",
        "type": "com.infa.ldm.relational.Table",
        "core": {
            "name": "customer_segment_snapshot",
            "classType": "com.infa.ldm.relational.Table",
        },
    },
}
_lineage_links: list[dict[str, str]] = []


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _decode_id(encoded_id: str) -> str:
    """Reverse the tilde-encoding used by InformaticaEDCHook._encode_id."""

    def replace_tilde(m: re.Match) -> str:
        return bytes.fromhex(m.group(1)).decode("utf-8")

    decoded = re.sub(r"~([0-9a-f]+)~", replace_tilde, encoded_id)
    return urllib.parse.unquote(decoded)


def _get_or_create_object(object_id: str) -> dict[str, Any]:
    if object_id not in _catalog:
        _catalog[object_id] = {
            "id": object_id,
            "name": object_id.split("/")[-1],
            "type": "com.infa.ldm.relational.Table",
            "core": {
                "name": object_id.split("/")[-1],
                "classType": "com.infa.ldm.relational.Table",
            },
        }
    return _catalog[object_id]


def _parse_class_types(filter_expr: str) -> set[str]:
    """Extract class types from expressions like 'core.classType:A OR core.classType:B'."""
    class_types: set[str] = set()
    for part in filter_expr.split(" OR "):
        candidate = part.strip()
        prefix = "core.classType:"
        if candidate.startswith(prefix):
            class_type = candidate[len(prefix) :].strip().strip('"')
            if class_type:
                class_types.add(class_type)
    return class_types


def _extract_filter_value(filter_expr: str, key: str) -> str | None:
    """Return value from a simple filter expression like 'core.name:orders'."""
    prefix = f"{key}:"
    if not filter_expr.startswith(prefix):
        return None
    return filter_expr[len(prefix) :].strip().strip('"')


# ---------------------------------------------------------------------------
# Health-check
# ---------------------------------------------------------------------------


@app.get("/access")
def health_check():
    """EDC connectivity check used by InformaticaEDCHook."""
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Catalog search  (GET /access/2/catalog/data/search)
# ---------------------------------------------------------------------------


@app.get("/access/2/catalog/data/search")
def edc_catalog_search(
    request: Request,
    q: str | None = None,
    fq: list[str] | None = None,
    fl: str = "core.name",
    defaultFacets: bool = True,
    disableSemanticSearch: bool = False,
    enableLegacySearch: bool = False,
    facet: bool = False,
    highlight: bool = False,
    includeRefObjects: bool = False,
):
    """
    Simulate EDC catalog search.

    Supports the fq filters used by InformaticaEDCHook:
    - core.externalId:<uri>
    - core.classType:<type> OR core.classType:<type>
    - core.name:<name>

    Multiple fq filters are combined with AND semantics.
    """
    class_type_filters: set[str] = set()
    core_name_filter: str | None = None
    external_id_filter: str | None = None

    raw_fq_list = request.query_params.getlist("fq")
    fq_filters = raw_fq_list if raw_fq_list else (fq or [])

    for filter_expr in fq_filters:
        external_id = _extract_filter_value(filter_expr, "core.externalId")
        if external_id is not None:
            external_id_filter = external_id
            continue

        core_name = _extract_filter_value(filter_expr, "core.name")
        if core_name is not None:
            core_name_filter = core_name
            continue

        class_type_filters.update(_parse_class_types(filter_expr))

    if external_id_filter:
        obj = _get_or_create_object(external_id_filter)
        hits = [obj]
    else:
        hits = list(_catalog.values())

    if class_type_filters:
        hits = [obj for obj in hits if obj.get("core", {}).get("classType") in class_type_filters]

    # Treat empty core.name filter as wildcard to mimic permissive EDC behavior.
    if core_name_filter:
        hits = [obj for obj in hits if obj.get("core", {}).get("name") == core_name_filter]

    return {
        "hits": hits,
        "facets": [],
        "count": len(hits),
        "queryTime": 1,
    }


# ---------------------------------------------------------------------------
# Fetch object by ID  (GET /access/2/catalog/data/objects/{object_id})
# ---------------------------------------------------------------------------


@app.get("/access/2/catalog/data/objects/{object_id:path}")
def get_catalog_object(object_id: str):
    """
    Return a catalog object by its (possibly tilde-encoded) ID.

    InformaticaEDCHook calls this endpoint after searching; the response must
    contain at least ``{"id": "<object_id>", ...}`` for lineage to proceed.
    """
    decoded_id = _decode_id(object_id)
    return _get_or_create_object(decoded_id)


# ---------------------------------------------------------------------------
# Create/update lineage  (PATCH /access/1/catalog/data/objects)
# ---------------------------------------------------------------------------


@app.patch("/access/1/catalog/data/objects")
async def update_catalog_objects(request: Request):
    """
    Accept a lineage-link payload from InformaticaEDCHook.create_lineage_link.

    Stores the relationship in memory and returns a minimal success envelope.
    """
    try:
        body: dict[str, Any] = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    updates = body.get("updates", [])
    for update in updates:
        target_id = update.get("id", "")
        for link in update.get("newSourceLinks", []):
            source_id = link.get("objectId", "")
            association = link.get("associationId", "core.DataSetDataFlow")
            _lineage_links.append(
                {
                    "source": source_id,
                    "target": target_id,
                    "association": association,
                }
            )

    return JSONResponse(
        status_code=200,
        content={"status": "success", "linksCreated": len(updates)},
    )


# ---------------------------------------------------------------------------
# Inspection endpoints (dev-only)
# ---------------------------------------------------------------------------


@app.get("/lineage")
def list_lineage():
    """Return all recorded lineage links (dev helper)."""
    return {"links": _lineage_links, "total": len(_lineage_links)}


@app.delete("/lineage")
def clear_lineage():
    """Clear all in-memory state (useful between test runs)."""
    _lineage_links.clear()
    _catalog.clear()
    return {"status": "cleared"}


@app.get("/catalog")
def list_catalog():
    """Return all known catalog objects (dev helper)."""
    return {"objects": list(_catalog.values()), "total": len(_catalog)}


# ---------------------------------------------------------------------------
# Legacy / compatibility endpoints kept from original simulator
# ---------------------------------------------------------------------------


class _Table(BaseModel):
    name: str
    columns: list[str]


class _Resource(BaseModel):
    name: str
    type: str
    tables: list[_Table] = []


_resources_db: dict[str, dict] = {}


@app.get("/resources")
def list_resources():
    return list(_resources_db.values())


@app.post("/resources")
def create_resource(resource: _Resource):
    resource_id = str(uuid.uuid4())
    _resources_db[resource_id] = resource.model_dump()
    return {"resource_id": resource_id}


@app.post("/resources/{resource_id}/tables")
def add_table(resource_id: str, table: _Table):
    if resource_id not in _resources_db:
        raise HTTPException(status_code=404, detail="Resource not found")
    _resources_db[resource_id]["tables"].append(table.model_dump())
    return table
