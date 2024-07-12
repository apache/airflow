from __future__ import annotations

from typing import NamedTuple

from marshmallow import Schema, fields
from marshmallow_sqlalchemy import SQLAlchemySchema, auto_field

from airflow.models.taskinstance import TaskEventLog


class TaskEventLogSchema(SQLAlchemySchema):
    """Event log schema."""

    class Meta:
        """Meta."""

        model = TaskEventLog

    id = auto_field(dump_only=True)
    dag_id = auto_field(dump_only=True)
    task_id = auto_field(dump_only=True)
    run_id = auto_field(dump_only=True)
    map_index = auto_field(dump_only=True)
    try_number = auto_field(dump_only=True)
    message = auto_field(dump_only=True)


class TaskEventLogCollection(NamedTuple):
    """List of import errors with metadata."""

    event_logs: list[TaskEventLog]
    total_entries: int


class TaskEventLogCollectionSchema(Schema):
    """EventLog Collection Schema."""

    event_logs = fields.List(fields.Nested(TaskEventLogSchema))
    task_event_logs = fields.List(fields.Nested(TaskEventLogSchema))
    total_entries = fields.Int()
