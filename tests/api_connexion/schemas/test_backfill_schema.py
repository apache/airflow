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

from airflow.api_connexion.schemas.backfill_schema import BackfillCollection, backfill_collection_schema
from airflow.models.backfill import Backfill
from airflow.utils import timezone


class TestBackfillSchema:
    def test_serialize_direct(self):
        now = timezone.utcnow()
        now_iso = now.isoformat()
        b1 = Backfill(
            dag_id="hi",
            created_at=now,
            completed_at=now,
            from_date=now,
            to_date=now,
            updated_at=now,
        )
        bc = BackfillCollection(backfills=[b1], total_entries=1)
        out = backfill_collection_schema.dump(bc)
        assert out == {
            "backfills": [
                {
                    "completed_at": now_iso,
                    "created_at": now_iso,
                    "dag_id": "hi",
                    "dag_run_conf": None,
                    "from_date": now_iso,
                    "id": None,
                    "is_paused": None,
                    "max_active_runs": None,
                    "to_date": now_iso,
                    "updated_at": now_iso,
                }
            ],
            "total_entries": 1,
        }
