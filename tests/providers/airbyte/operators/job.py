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

job_example = {
    "job": {
        "id": 135,
        "configType": "sync",
        "configId": "166e2d70-02ff-478f-9259-7b0985c945ff",
        "enabledStreams": [
            {"name": "vehicle", "namespace": "public"},
            {"name": "location", "namespace": "public"},
        ],
        "createdAt": 1720467640,
        "updatedAt": 1720467663,
        "status": "succeeded",
        "streamAggregatedStats": [],
    },
    "attempts": [
        {
            "attempt": {
                "id": 0,
                "status": "succeeded",
                "createdAt": 1720467640,
                "updatedAt": 1720467663,
                "endedAt": 1720467663,
                "bytesSynced": 0,
                "recordsSynced": 0,
                "totalStats": {
                    "recordsEmitted": 0,
                    "bytesEmitted": 0,
                    "stateMessagesEmitted": 2,
                    "recordsCommitted": 0,
                },
                "streamStats": [
                    {
                        "streamName": "vehicle",
                        "stats": {"recordsEmitted": 0, "bytesEmitted": 0, "recordsCommitted": 0},
                    },
                    {
                        "streamName": "location",
                        "stats": {"recordsEmitted": 0, "bytesEmitted": 0, "recordsCommitted": 0},
                    },
                ],
            },
            "logs": {"logLines": []},
        }
    ],
}
