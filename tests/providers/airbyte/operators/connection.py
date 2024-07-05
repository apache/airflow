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

connection_nested = {
    "connectionId": "ddedbe1b-9985-4735-b95e-e240304bdb0c",
    "name": "PokeAPI → Snowflake",
    "namespaceDefinition": "destination",
    "prefix": "",
    "sourceId": "a888ff73-e04b-46fa-9e96-563fed57e8f6",
    "destinationId": "d401bafb-aa51-4c70-8a1c-1aa07f3d0a2d",
    "operationIds": [],
    "syncCatalog": {
        "streams": [
            {
                "stream": {
                    "name": "pokemon",
                    "jsonSchema": {
                        "type": "object",
                        "$schema": "http://json-schema.org/draft-07/schema#",
                        "properties": {
                            "order": {"type": ["null", "integer"]},
                            "weight": {"type": ["null", "integer"]},
                            "species": {
                                "type": ["null", "object"],
                                "properties": {
                                    "url": {"type": ["null", "string"]},
                                    "name": {"type": ["null", "string"]},
                                },
                                "additionalProperties": True,
                            },
                            "is_default": {"type": ["null", "boolean"]},
                            "past_types": {
                                "type": ["null", "array"],
                                "items": {
                                    "type": ["null", "object"],
                                    "properties": {
                                        "types": {
                                            "type": ["null", "array"],
                                            "items": {
                                                "type": ["null", "object"],
                                                "properties": {
                                                    "slot": {"type": ["null", "integer"]},
                                                    "type": {
                                                        "type": ["null", "object"],
                                                        "properties": {
                                                            "url": {"type": ["null", "string"]},
                                                            "name": {"type": ["null", "string"]},
                                                        },
                                                        "additionalProperties": True,
                                                    },
                                                },
                                                "additionalProperties": True,
                                            },
                                        },
                                        "generation": {
                                            "type": ["null", "object"],
                                            "properties": {
                                                "url": {"type": ["null", "string"]},
                                                "name": {"type": ["null", "string"]},
                                            },
                                            "additionalProperties": True,
                                        },
                                    },
                                    "additionalProperties": True,
                                },
                            },
                            "base_experience": {"type": ["null", "integer"]},
                            "location_area_encounters": {"type": ["null", "string"]},
                        },
                        "additionalProperties": True,
                    },
                    "supportedSyncModes": ["full_refresh"],
                    "sourceDefinedCursor": False,
                    "defaultCursorField": [],
                    "sourceDefinedPrimaryKey": [["id"]],
                },
                "config": {
                    "syncMode": "full_refresh",
                    "cursorField": [],
                    "destinationSyncMode": "overwrite",
                    "primaryKey": [["id"]],
                    "aliasName": "pokemon",
                    "selected": True,
                    "suggested": False,
                    "fieldSelectionEnabled": True,
                    "selectedFields": [
                        {"fieldPath": ["order"]},
                        {"fieldPath": ["weight"]},
                        {"fieldPath": ["species"]},
                        {"fieldPath": ["is_default"]},
                        {"fieldPath": ["past_types"]},
                        {"fieldPath": ["base_experience"]},
                        {"fieldPath": ["location_area_encounters"]},
                    ],
                },
            }
        ]
    },
    "scheduleType": "manual",
    "status": "active",
    "sourceCatalogId": "af049f10-5351-4d8a-832e-b717404714e1",
    "geography": "auto",
    "breakingChange": False,
    "notifySchemaChanges": True,
    "notifySchemaChangesByEmail": False,
    "nonBreakingChangesPreference": "propagate_columns",
    "created_at": 1720089897,
    "backfillPreference": "disabled",
}


connection_flat = {
    "connectionId": "166e2d70-02ff-478f-9259-7b0985c945ff",
    "name": "Postgres → Snowflake",
    "namespaceDefinition": "customformat",
    "namespaceFormat": "new_schema",
    "prefix": "sync_",
    "sourceId": "574b08f1-6c9e-46be-95e5-8a1669ba0847",
    "destinationId": "d401bafb-aa51-4c70-8a1c-1aa07f3d0a2d",
    "operationIds": [],
    "syncCatalog": {
        "streams": [
            {
                "stream": {
                    "name": "vehicle",
                    "jsonSchema": {
                        "type": "object",
                        "properties": {
                            "id": {"type": "string"},
                            "number": {"type": "number", "airbyte_type": "integer"},
                            "created_at": {
                                "type": "string",
                                "format": "date-time",
                                "airbyte_type": "timestamp_with_timezone",
                            },
                        },
                    },
                    "supportedSyncModes": ["full_refresh", "incremental"],
                    "sourceDefinedCursor": False,
                    "defaultCursorField": [],
                    "sourceDefinedPrimaryKey": [["id"]],
                    "namespace": "public",
                    "isResumable": True,
                },
                "config": {
                    "syncMode": "incremental",
                    "cursorField": ["created_at"],
                    "destinationSyncMode": "append_dedup",
                    "primaryKey": [["id"]],
                    "aliasName": "vehicle",
                    "selected": True,
                    "suggested": False,
                    "fieldSelectionEnabled": False,
                    "selectedFields": [],
                },
            },
            {
                "stream": {
                    "name": "location",
                    "jsonSchema": {
                        "type": "object",
                        "properties": {
                            "id": {"type": "string"},
                            "lat": {"type": "number"},
                            "lon": {"type": "number"},
                            "name": {"type": "string"},
                            "created_at": {
                                "type": "string",
                                "format": "date-time",
                                "airbyte_type": "timestamp_with_timezone",
                            },
                        },
                    },
                    "supportedSyncModes": ["full_refresh", "incremental"],
                    "sourceDefinedCursor": False,
                    "defaultCursorField": [],
                    "sourceDefinedPrimaryKey": [["id"]],
                    "namespace": "public",
                    "isResumable": True,
                },
                "config": {
                    "syncMode": "incremental",
                    "cursorField": ["created_at"],
                    "destinationSyncMode": "append_dedup",
                    "primaryKey": [["id"]],
                    "aliasName": "location",
                    "selected": True,
                    "suggested": False,
                    "fieldSelectionEnabled": False,
                    "selectedFields": [],
                },
            },
        ]
    },
    "scheduleType": "manual",
    "status": "active",
    "sourceCatalogId": "b7fc3662-527b-4a8e-a7fa-e0469ce9ba66",
    "geography": "auto",
    "breakingChange": False,
    "notifySchemaChanges": True,
    "notifySchemaChangesByEmail": False,
    "nonBreakingChangesPreference": "propagate_columns",
    "created_at": 1719940532,
    "backfillPreference": "disabled",
}

sync_catalog_is_missing = {
    "connectionId": "166e2d70-02ff-478f-9259-7b0985c945ff",
    "name": "Postgres → Snowflake",
    "namespaceDefinition": "customformat",
    "namespaceFormat": "new_schema",
    "prefix": "sync_",
    "sourceId": "574b08f1-6c9e-46be-95e5-8a1669ba0847",
    "destinationId": "d401bafb-aa51-4c70-8a1c-1aa07f3d0a2d",
    "operationIds": [],
    "scheduleType": "manual",
    "status": "active",
    "sourceCatalogId": "b7fc3662-527b-4a8e-a7fa-e0469ce9ba66",
    "geography": "auto",
    "breakingChange": False,
    "notifySchemaChanges": True,
    "notifySchemaChangesByEmail": False,
    "nonBreakingChangesPreference": "propagate_columns",
    "created_at": 1719940532,
    "backfillPreference": "disabled",
}

streams_are_missing = {
    "connectionId": "166e2d70-02ff-478f-9259-7b0985c945ff",
    "name": "Postgres → Snowflake",
    "namespaceDefinition": "customformat",
    "namespaceFormat": "new_schema",
    "prefix": "sync_",
    "sourceId": "574b08f1-6c9e-46be-95e5-8a1669ba0847",
    "destinationId": "d401bafb-aa51-4c70-8a1c-1aa07f3d0a2d",
    "operationIds": [],
    "scheduleType": "manual",
    "status": "active",
    "sourceCatalogId": "b7fc3662-527b-4a8e-a7fa-e0469ce9ba66",
    "geography": "auto",
    "breakingChange": False,
    "notifySchemaChanges": True,
    "notifySchemaChangesByEmail": False,
    "nonBreakingChangesPreference": "propagate_columns",
    "created_at": 1719940532,
    "backfillPreference": "disabled",
    "syncCatalog": {},
}

empty_streams = {
    "connectionId": "166e2d70-02ff-478f-9259-7b0985c945ff",
    "name": "Postgres → Snowflake",
    "namespaceDefinition": "customformat",
    "namespaceFormat": "new_schema",
    "prefix": "sync_",
    "sourceId": "574b08f1-6c9e-46be-95e5-8a1669ba0847",
    "destinationId": "d401bafb-aa51-4c70-8a1c-1aa07f3d0a2d",
    "operationIds": [],
    "scheduleType": "manual",
    "status": "active",
    "sourceCatalogId": "b7fc3662-527b-4a8e-a7fa-e0469ce9ba66",
    "geography": "auto",
    "breakingChange": False,
    "notifySchemaChanges": True,
    "notifySchemaChangesByEmail": False,
    "nonBreakingChangesPreference": "propagate_columns",
    "created_at": 1719940532,
    "backfillPreference": "disabled",
    "syncCatalog": {"streams": [{}]},
}

no_json_schema = {
    "connectionId": "ddedbe1b-9985-4735-b95e-e240304bdb0c",
    "name": "PokeAPI → Snowflake",
    "namespaceDefinition": "destination",
    "prefix": "",
    "sourceId": "a888ff73-e04b-46fa-9e96-563fed57e8f6",
    "destinationId": "d401bafb-aa51-4c70-8a1c-1aa07f3d0a2d",
    "operationIds": [],
    "syncCatalog": {
        "streams": [
            {
                "stream": {
                    "name": "pokemon",
                    "supportedSyncModes": ["full_refresh"],
                    "sourceDefinedCursor": False,
                    "defaultCursorField": [],
                    "sourceDefinedPrimaryKey": [["id"]],
                },
                "config": {
                    "syncMode": "full_refresh",
                    "cursorField": [],
                    "destinationSyncMode": "overwrite",
                    "primaryKey": [["id"]],
                    "aliasName": "pokemon",
                    "selected": True,
                    "suggested": False,
                    "fieldSelectionEnabled": True,
                    "selectedFields": [
                        {"fieldPath": ["order"]},
                        {"fieldPath": ["weight"]},
                        {"fieldPath": ["species"]},
                        {"fieldPath": ["is_default"]},
                        {"fieldPath": ["past_types"]},
                        {"fieldPath": ["base_experience"]},
                        {"fieldPath": ["location_area_encounters"]},
                    ],
                },
            }
        ]
    },
    "scheduleType": "manual",
    "status": "active",
    "sourceCatalogId": "af049f10-5351-4d8a-832e-b717404714e1",
    "geography": "auto",
    "breakingChange": False,
    "notifySchemaChanges": True,
    "notifySchemaChangesByEmail": False,
    "nonBreakingChangesPreference": "propagate_columns",
    "created_at": 1720089897,
    "backfillPreference": "disabled",
}


no_properties = {
    "connectionId": "ddedbe1b-9985-4735-b95e-e240304bdb0c",
    "name": "PokeAPI → Snowflake",
    "namespaceDefinition": "destination",
    "prefix": "",
    "sourceId": "a888ff73-e04b-46fa-9e96-563fed57e8f6",
    "destinationId": "d401bafb-aa51-4c70-8a1c-1aa07f3d0a2d",
    "operationIds": [],
    "syncCatalog": {
        "streams": [
            {
                "stream": {
                    "name": "pokemon",
                    "jsonSchema": {
                        "type": "object",
                        "$schema": "http://json-schema.org/draft-07/schema#",
                        "additionalProperties": True,
                    },
                    "supportedSyncModes": ["full_refresh"],
                    "sourceDefinedCursor": False,
                    "defaultCursorField": [],
                    "sourceDefinedPrimaryKey": [["id"]],
                },
                "config": {
                    "syncMode": "full_refresh",
                    "cursorField": [],
                    "destinationSyncMode": "overwrite",
                    "primaryKey": [["id"]],
                    "aliasName": "pokemon",
                    "selected": True,
                    "suggested": False,
                    "fieldSelectionEnabled": True,
                    "selectedFields": [
                        {"fieldPath": ["order"]},
                        {"fieldPath": ["weight"]},
                        {"fieldPath": ["species"]},
                        {"fieldPath": ["is_default"]},
                        {"fieldPath": ["past_types"]},
                        {"fieldPath": ["base_experience"]},
                        {"fieldPath": ["location_area_encounters"]},
                    ],
                },
            }
        ]
    },
    "scheduleType": "manual",
    "status": "active",
    "sourceCatalogId": "af049f10-5351-4d8a-832e-b717404714e1",
    "geography": "auto",
    "breakingChange": False,
    "notifySchemaChanges": True,
    "notifySchemaChangesByEmail": False,
    "nonBreakingChangesPreference": "propagate_columns",
    "created_at": 1720089897,
    "backfillPreference": "disabled",
}
