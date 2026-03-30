# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file

from __future__ import annotations

import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

import attrs
import pendulum
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Attempt to load standard log structures according to Airflow 3 requirements
from airflow.providers.common.compat.sdk import conf
from airflow.utils.log.file_task_handler import FileTaskHandler 
from airflow.utils.log.logging_mixin import ExternalLoggingMixin, LoggingMixin

# Try mapping for StructuredLogMessage available in 3.x
try:
    from airflow.utils.log.file_task_handler import StructuredLogMessage
except ImportError:
    StructuredLogMessage = dict  # Fallback for compilation matching

# Try loading version compat constants
try:
    from airflow.providers.elasticsearch.version_compat import AIRFLOW_V_3_0_PLUS, AIRFLOW_V_3_2_PLUS
except ImportError:
    AIRFLOW_V_3_0_PLUS = True
    AIRFLOW_V_3_2_PLUS = True

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstance
    from airflow.sdk.types import RuntimeTaskInstanceProtocol as RuntimeTI
    from airflow.utils.log.file_task_handler import LogMessages, LogMetadata, LogSourceInfo


def _render_log_labels(ti) -> dict[str, str]:
    """
    Helper to extract low-cardinality labels for Loki streams.
    High-cardinality fields (like task_id, run_id) are omitted here
    to prevent stream explosion and will be indexed via Bloom filters instead.
    """
    return {
        "job": "airflow_tasks",
        "dag_id": ti.dag_id,
    }

@attrs.define(kw_only=True)
class LokiRemoteLogIO(LoggingMixin):
    """
    Handles the actual communication with Loki API.
    Used by Task Supervisor to bulk-upload logs and by UI to read remote logs.
    """
    host: str = "http://localhost:3100"
    base_log_folder: Path = attrs.field(converter=Path)
    delete_local_copy: bool = False
    
    @property
    def session(self) -> requests.Session:
        if not hasattr(self, "_session"):
            self._session = requests.Session()
            # Implementing Retries, Jitter, and Exponential Backoff via urllib3's Retry
            retries = Retry(
                total=5,
                backoff_factor=1,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["GET", "POST"]
            )
            # Efficient scaling with TCP connection pooling 
            adapter = HTTPAdapter(max_retries=retries, pool_connections=20, pool_maxsize=100)
            self._session.mount("http://", adapter)
            self._session.mount("https://", adapter)
        return self._session

    def upload(self, path: os.PathLike | str, ti: RuntimeTI):
        """Called by Airflow Task Supervisor after task finishes (or during) to push logs."""
        path = Path(path)
        local_loc = path if path.is_absolute() else self.base_log_folder.joinpath(path)

        if not local_loc.is_file():
            return

        # Read the raw JSON log lines produced by the Airflow 3 Task Supervisor
        raw_logs = local_loc.read_text().splitlines()
        
        # Prepare the payload for Loki (Loki Push API)
        labels = _render_log_labels(ti)
        values = []
        for line in raw_logs:
            if not line.strip():
                continue
            
            try:
                # Log line content from Task Supervisor
                log_data = json.loads(line)
                
                # Inject high-cardinality contextual fields into the JSON payload.
                log_data["task_id"] = ti.task_id
                log_data["run_id"] = getattr(ti, "run_id", "")
                log_data["try_number"] = str(ti.try_number)
                log_data["map_index"] = str(getattr(ti, "map_index", -1))

                # Loki expects Timestamp in nanoseconds as string
                timestamp_ns = str(int(time.time() * 1e9)) 
                values.append([timestamp_ns, json.dumps(log_data)])
            except Exception:
                pass
                
        payload = {
            "streams": [
                {
                    "stream": labels,
                    "values": values
                }
            ]
        }
        
        # Push to Loki using configured reliable session
        try:
            resp = self.session.post(f"{self.host}/loki/api/v1/push", json=payload, timeout=(3.0, 15.0))
            resp.raise_for_status()
            
            # Clean up local file just like ElasticsearchRemoteLogIO does
            if self.delete_local_copy:
                import shutil
                shutil.rmtree(local_loc.parent, ignore_errors=True)
        except Exception as e:
            self.log.exception("Failed to upload logs to Loki: %s", e)

    def read(self, _relative_path: str, ti: RuntimeTI) -> tuple[LogSourceInfo, LogMessages]:
        """Fetch logs from Loki using LogQL for streaming or retrieval."""
        labels = _render_log_labels(ti)
        
        # 1. Base stream selector (hits low-cardinality index)
        stream_selector = "{" + ",".join([f'{k}="{v}"' for k, v in labels.items()]) + "}"
        
        # 2. Line filters (leveraging Loki Bloom filters)
        run_id = getattr(ti, "run_id", "")
        try_num = str(ti.try_number)
        map_idx = str(getattr(ti, "map_index", -1))
        
        # Utilizing Loki's `| json` parser and exact match filters for maximum TSDB optimization
        logQL = (
            f"{stream_selector} "
            f'| json '
            f'| task_id="{ti.task_id}" '
            f'| run_id="{run_id}" '
            f'| try_number="{try_num}" '
            f'| map_index="{map_idx}"'
        )
        
        # Query Loki API using configured reliable session
        resp = self.session.get(f"{self.host}/loki/api/v1/query_range", params={"query": logQL}, timeout=(3.0, 15.0))
        
        message = []
        if resp.ok:
            data = resp.json().get("data", {}).get("result", [])
            for stream in data:
                for val in stream.get("values", []):
                    # parse the underlying JSON structured log we uploaded
                    log_entry = json.loads(val[1])
                    message.append(json.dumps(log_entry))
        
        return ["loki-remote"], message


class LokiTaskHandler(FileTaskHandler, ExternalLoggingMixin, LoggingMixin):
    """
    The main logging handler injected into Airflow configuration.
    """
    LOG_NAME = "Loki"

    def __init__(self, base_log_folder: str, host: str, frontend: str = "", **kwargs):
        super().__init__(base_log_folder=base_log_folder, **kwargs)
        self.host = host
        self.frontend = frontend
        self.io = LokiRemoteLogIO(
            host=self.host,
            base_log_folder=base_log_folder,
            delete_local_copy=kwargs.get("delete_local_copy", False),
        )
        
        # Register Remote Log IO globally for Airflow 3 Task Supervisor
        if AIRFLOW_V_3_0_PLUS:
            if AIRFLOW_V_3_2_PLUS:
                try:
                    from airflow.logging_config import _ActiveLoggingConfig, get_remote_task_log
                    if get_remote_task_log() is None:
                        _ActiveLoggingConfig.set(self.io, None)
                except ImportError:
                    pass
            else:
                try:
                    import airflow.logging_config as alc
                    if getattr(alc, "REMOTE_TASK_LOG", None) is None:
                        alc.REMOTE_TASK_LOG = self.io
                except ImportError:
                    pass
                    
    def _read(
        self, ti: TaskInstance, try_number: int, metadata: LogMetadata | None = None
    ) -> tuple[list[Any] | str, dict[str, Any]]:
        """
        Implementation of the log read handler invoked by the Web UI.
        Returns a list of StructuredLogMessage objects in Airflow 3+.
        """
        metadata = metadata or {"offset": 0}
        
        headers, messages = self.io.read("", ti)
        
        structured_messages = []
        structured_messages.append(StructuredLogMessage(event="::group::Loki logs", sources=headers))
        for msg in messages:
            try:
                log_data = json.loads(msg)
                if "event" not in log_data and "message" in log_data:
                    log_data["event"] = log_data.pop("message")
                structured_messages.append(StructuredLogMessage(**log_data))
            except Exception:
                structured_messages.append(StructuredLogMessage(event=msg))
                
        structured_messages.append(StructuredLogMessage(event="::endgroup::"))

        # Mark end of log if task is done and no more records
        metadata["end_of_log"] = True 
        
        return structured_messages, metadata

    @property
    def supports_external_link(self) -> bool:
        """Let Airflow API Server know if we can return a link to Grafana."""
        return bool(self.frontend)

    def get_external_log_url(self, task_instance: TaskInstance, try_number: int) -> str:
        """
        Used by `airflow-api-server` when users request the external log URL.
        Constructs a direct link to Grafana Explorer view for these logs.
        """
        if not self.frontend:
            return ""
            
        import urllib.parse
        
        labels = _render_log_labels(task_instance)
        stream_selector = "{" + ",".join([f'{k}="{v}"' for k, v in labels.items()]) + "}"
        logQL = (
            f'{stream_selector} '
            f'| json '
            f'| task_id="{task_instance.task_id}" '
            f'| try_number="{try_number}" '
        )
        if hasattr(task_instance, "run_id"):
            logQL += f'| run_id="{task_instance.run_id}" '
            
        params = urllib.parse.urlencode({"left": json.dumps(["now-1h", "now", "Loki", {"expr": logQL}])})
        
        grafana_url = self.frontend if self.frontend.endswith("/") else self.frontend + "/"
        return f"{grafana_url}explore?{params}"
