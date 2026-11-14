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

from datetime import datetime, timedelta

import pytest
from sqlalchemy import select

from airflow._shared.timezones import timezone
from airflow.assets.manager import AssetManager
from airflow.models.asset import AssetActive, AssetModel
from airflow.models.callback import Callback
from airflow.models.dag import DagModel
from airflow.models.dagbundle import DagBundleModel
from airflow.models.deadline import Deadline
from airflow.models.deadline_alert import DeadlineAlert
from airflow.models.serialized_dag import SerializedDagModel
from airflow.sdk import DAG, Asset, AssetDeadline, SyncCallback
from airflow.sdk.bases.operator import BaseOperator
from airflow.serialization.decoders import _decode_deadline_callback
from airflow.serialization.encoders import encode_asset_deadline
from airflow.serialization.serialized_objects import DagSerialization, LazyDeserializedDAG
from airflow.utils.state import CallbackState

pytestmark = pytest.mark.db_test


def _sample_callback(**context):
    pass


def _resolve_callback(**context):
    pass


class _DummyOperator(BaseOperator):
    def execute(self, context):
        pass


class TestAssetDeadlineLifecycle:
    def test_proactive_interval_scheduling_and_pruning_on_event(self, session):
        asset_model = AssetModel(name="orders_daily", uri="s3://bucket/orders_daily", active=AssetActive())
        session.add(asset_model)
        session.flush()

        sdk_dl = AssetDeadline(
            interval=timedelta(hours=6), callback=SyncCallback(_sample_callback), name="sla_6h"
        )
        encoded = encode_asset_deadline(sdk_dl)

        t0 = timezone.utcnow() - timedelta(hours=1)
        alert = DeadlineAlert(
            asset_id=asset_model.id,
            is_producer=True,
            name="sla_6h",
            reference=encoded["reference"],
            cron=None,
            interval=encoded["interval"],
            callback_def=encoded["callback"],
        )
        alert.created_at = t0
        session.add(alert)
        session.flush()

        created = Deadline.schedule_asset_deadlines(session=session, asset_id=asset_model.id, now=t0)
        assert created == 1
        assert Deadline.schedule_asset_deadlines(session=session, asset_id=asset_model.id, now=t0) == 0

        deadlines = session.scalars(select(Deadline).where(Deadline.asset_id == asset_model.id)).all()
        assert len(deadlines) == 1
        assert deadlines[0].deadline_time == t0 + timedelta(hours=6)
        assert not deadlines[0].missed
        assert not deadlines[0].is_late

        AssetManager.register_asset_change(
            asset=Asset(name="orders_daily", uri="s3://bucket/orders_daily"),
            session=session,
        )
        remaining = session.scalars(select(Deadline).where(Deadline.asset_id == asset_model.id)).all()
        assert len(remaining) == 1
        assert remaining[0].deadline_time > t0 + timedelta(hours=6)

    def test_late_recovery_and_on_resolve_callback(self, session):
        asset_model = AssetModel(name="late_asset", uri="s3://bucket/late_asset", active=AssetActive())
        session.add(asset_model)
        session.flush()

        sdk_dl = AssetDeadline(
            interval=timedelta(hours=2),
            callback=SyncCallback(_sample_callback),
            on_resolve=SyncCallback(_resolve_callback),
            name="recoverable_sla",
        )
        encoded = encode_asset_deadline(sdk_dl)
        alert = DeadlineAlert(
            asset_id=asset_model.id,
            is_producer=True,
            name="recoverable_sla",
            reference=encoded["reference"],
            cron=None,
            interval=encoded["interval"],
            callback_def=encoded["callback"],
            on_resolve_callback_def=encoded["on_resolve"],
        )
        session.add(alert)
        session.flush()

        cb_def = _decode_deadline_callback(alert.callback_def)
        dl = Deadline(
            deadline_time=timezone.utcnow() - timedelta(minutes=15),
            callback=cb_def,
            asset_id=asset_model.id,
            deadline_alert_id=alert.id,
        )
        session.add(dl)
        session.flush()

        dl.handle_miss(session=session)
        session.flush()
        assert dl.missed is True
        assert dl.resolved_at is None
        assert dl.is_late is True

        AssetManager.register_asset_change(
            asset=Asset(name="late_asset", uri="s3://bucket/late_asset"),
            session=session,
        )
        session.refresh(dl)
        assert dl.missed is True
        assert dl.resolved_at is not None
        assert dl.is_late is False

        callbacks = session.scalars(select(Callback)).all()
        resolve_cbs = [c for c in callbacks if "_resolve_callback" in c.data.get("path", "")]
        assert len(resolve_cbs) == 1
        assert resolve_cbs[0].data["kwargs"]["context"]["resolved"] is True

    def test_partitioned_discovery_pruning_and_handle_miss_context(self, session):
        asset_model = AssetModel(name="events_part", uri="s3://bucket/events_part", active=AssetActive())
        session.add(asset_model)
        session.flush()

        AssetManager.register_asset_change(
            asset=Asset(name="events_part", uri="s3://bucket/events_part"),
            partition_key="2026-11-13",
            session=session,
        )
        AssetManager.register_asset_change(
            asset=Asset(name="events_part", uri="s3://bucket/events_part"),
            partition_key="2026-11-14",
            session=session,
        )

        sdk_dl = AssetDeadline(cron="0 9 * * *", callback=SyncCallback(_sample_callback))
        encoded = encode_asset_deadline(sdk_dl)
        alert = DeadlineAlert(
            asset_id=asset_model.id,
            is_producer=True,
            reference=encoded["reference"],
            cron="0 9 * * *",
            interval=None,
            callback_def=encoded["callback"],
        )
        session.add(alert)
        session.flush()

        cb_def = _decode_deadline_callback(alert.callback_def)
        due_time = timezone.utcnow() + timedelta(hours=2)
        d1 = Deadline(
            deadline_time=due_time,
            callback=cb_def,
            asset_id=asset_model.id,
            partition_key="2026-11-13",
            deadline_alert_id=alert.id,
        )
        d2 = Deadline(
            deadline_time=due_time,
            callback=cb_def,
            asset_id=asset_model.id,
            partition_key="2026-11-14",
            deadline_alert_id=alert.id,
        )
        session.add_all([d1, d2])
        session.flush()

        AssetManager.register_asset_change(
            asset=Asset(name="events_part", uri="s3://bucket/events_part"),
            partition_key="2026-11-13",
            session=session,
        )
        assert session.get(Deadline, d1.id) is None
        assert session.get(Deadline, d2.id) is not None

        d2.handle_miss(session=session)
        session.flush()
        assert d2.missed is True
        assert d2.callback.state == CallbackState.PENDING
        ctx = d2.callback.data["kwargs"]["context"]
        assert ctx["partition_key"] == "2026-11-14"
        assert ctx["missing_partition_keys"] == ["2026-11-14"]

    def test_per_dag_ownership_with_pooled_deadline_evaluation(self, session):
        session.merge(DagBundleModel(name="dags-folder"))
        session.flush()
        session.add_all(
            [
                DagModel(dag_id="consumer_dag", bundle_name="dags-folder"),
                DagModel(dag_id="producer_dag", bundle_name="dags-folder"),
            ]
        )
        asset_model = AssetModel(name="shared_asset", uri="s3://bucket/shared_asset", active=AssetActive())
        session.add(asset_model)
        session.flush()

        shared_dl = AssetDeadline(
            cron="0 9 * * *", callback=SyncCallback(_sample_callback), name="shared_sla"
        )
        shared_asset = Asset(name="shared_asset", uri="s3://bucket/shared_asset", deadline=shared_dl)

        with DAG(
            dag_id="consumer_dag", schedule=[shared_asset], start_date=datetime(2026, 1, 1)
        ) as consumer_dag:
            _DummyOperator(task_id="consume")

        with DAG(dag_id="producer_dag", schedule="@daily", start_date=datetime(2026, 1, 1)) as producer_dag:
            _DummyOperator(task_id="produce", outlets=[shared_asset])

        ser_consumer = LazyDeserializedDAG(data=DagSerialization.to_dict(consumer_dag))
        ser_producer = LazyDeserializedDAG(data=DagSerialization.to_dict(producer_dag))

        SerializedDagModel.write_dag(ser_consumer, bundle_name="dags-folder", session=session)
        SerializedDagModel.write_dag(ser_producer, bundle_name="dags-folder", session=session)

        alerts = session.scalars(select(DeadlineAlert).where(DeadlineAlert.asset_id == asset_model.id)).all()
        assert len(alerts) == 2
        assert {a.is_producer for a in alerts} == {False, True}

        deadlines = session.scalars(select(Deadline).where(Deadline.asset_id == asset_model.id)).all()
        assert len(deadlines) == 1


class TestAssetDeadlinesEndpoint:
    def test_get_asset_deadlines_endpoint(self, session):
        from fastapi import HTTPException

        from airflow.api_fastapi.core_api.routes.public.assets import get_asset_deadlines

        asset_model = AssetModel(name="api_asset", uri="s3://bucket/api_asset", active=AssetActive())
        session.add(asset_model)
        session.flush()

        sdk_dl = AssetDeadline(cron="0 9 * * *", callback=SyncCallback(_sample_callback), name="api_sla")
        encoded = encode_asset_deadline(sdk_dl)
        alert = DeadlineAlert(
            asset_id=asset_model.id,
            is_producer=True,
            name="api_sla",
            reference=encoded["reference"],
            cron="0 9 * * *",
            interval=None,
            callback_def=encoded["callback"],
        )
        session.add(alert)
        session.flush()

        Deadline.schedule_asset_deadlines(session=session, asset_id=asset_model.id)

        resp = get_asset_deadlines(asset_id=asset_model.id, session=session)
        assert len(resp.alerts) == 1
        assert resp.alerts[0].name == "api_sla"
        assert resp.alerts[0].is_producer is True
        assert len(resp.deadlines) == 1
        assert resp.deadlines[0].missed is False
        assert resp.deadlines[0].is_late is False
        assert resp.deadlines[0].resolved_at is None

        with pytest.raises(HTTPException) as exc_info:
            get_asset_deadlines(asset_id=999999, session=session)
        assert exc_info.value.status_code == 404


class TestPartitionedAssetTimetableRunDeadline:
    def test_multi_asset_run_deadline_stored_at_dag_level(self, session):
        from airflow.sdk import DAG, Asset, PartitionedAssetTimetable
        from airflow.sdk.bases.operator import BaseOperator

        class _DummyOperator(BaseOperator):
            def execute(self, context):
                pass

        session.merge(DagBundleModel(name="dags-folder"))
        session.flush()
        session.add(DagModel(dag_id="multi_asset_consumer", bundle_name="dags-folder"))
        orders = AssetModel(name="orders_daily", uri="s3://bucket/orders_daily", active=AssetActive())
        customers = AssetModel(
            name="customers_daily", uri="s3://bucket/customers_daily", active=AssetActive()
        )
        session.add_all([orders, customers])
        session.flush()

        sdk_orders = Asset(name="orders_daily", uri="s3://bucket/orders_daily")
        sdk_customers = Asset(name="customers_daily", uri="s3://bucket/customers_daily")
        run_dl = AssetDeadline(
            interval=timedelta(hours=8), callback=SyncCallback(_sample_callback), name="run_sla"
        )

        with DAG(
            dag_id="multi_asset_consumer",
            schedule=PartitionedAssetTimetable(assets=sdk_orders & sdk_customers, run_deadline=run_dl),
            start_date=datetime(2026, 1, 1),
        ) as dag:
            _DummyOperator(task_id="join_assets")

        ser_dag = LazyDeserializedDAG(data=DagSerialization.to_dict(dag))
        SerializedDagModel.write_dag(ser_dag, bundle_name="dags-folder", session=session)
        session.flush()

        alerts = session.scalars(select(DeadlineAlert).where(DeadlineAlert.name == "run_sla")).all()
        assert len(alerts) == 1
        assert alerts[0].asset_id is None
        assert alerts[0].serialized_dag_id is not None
        assert alerts[0].is_producer is False
