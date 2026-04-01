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
    processors: list = attrs.field(factory=list)
    
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

        labels = _render_log_labels(ti)
        values = []
        payload_size = 0
        MAX_PAYLOAD_SIZE = 1048576  # 1 MiB chunking as per Promtail limits

        def _push_chunk():
            if not values:
                return True
            payload = {
                "streams": [
                    {
                        "stream": labels,
                        "values": values
                    }
                ]
            }
            try:
                resp = self.session.post(f"{self.host}/loki/api/v1/push", json=payload, timeout=(3.0, 15.0))
                resp.raise_for_status()
                return True
            except Exception as e:
                self.log.exception("Failed to upload chunk of logs to Loki: %s", e)
                return False

        has_error = False

        with open(local_loc, "r") as f:
            for line in f:
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
                    log_str = json.dumps(log_data)
                    values.append([timestamp_ns, log_str])
                    
                    # Estimate the byte size of this entry in the payload
                    payload_size += len(timestamp_ns) + len(log_str) + 10 # 10 bytes overhead per value

                    if payload_size >= MAX_PAYLOAD_SIZE:
                        if not _push_chunk():
                            has_error = True
                        values.clear()
                        payload_size = 0

                except Exception:
                    pass

        # Push any remaining logs
        if values:
            if not _push_chunk():
                has_error = True

        # Clean up local file just like ElasticsearchRemoteLogIO does if fully successful
        if self.delete_local_copy and not has_error:
            try:
                import shutil
                shutil.rmtree(local_loc.parent, ignore_errors=True)
            except Exception:
                pass

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

    @property
    def log_name(self) -> str:
        return self.LOG_NAME

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
                    from airflow.logging_config import _ActiveLoggingConfig
                    try:
                        from airflow.logging_config import get_remote_task_log
                        if callable(get_remote_task_log) and get_remote_task_log() is None:
                            _ActiveLoggingConfig.set(self.io, None)
                    except ImportError:
                        pass
                except ImportError:
                    pass
            else:
                try:
                    import airflow.logging_config as alc
                    if getattr(alc, "REMOTE_TASK_LOG", None) is None:
                        alc.REMOTE_TASK_LOG = self.io
                except ImportError:
                    pass
                    
    def _read_remote_logs(self, ti: TaskInstance, try_number: int, metadata: dict | None = None) -> tuple[list[str], list[str]]:
        """
        Called by Airflow 3.x FileTaskHandler._read to fetch remote logs.
        Airflow 3 native FileTaskHandler manages interleaving these with locally streaming worker logs.
        """
        return self.io.read("", ti)

    def _read(
        self, ti: TaskInstance, try_number: int, metadata: LogMetadata | None = None
    ) -> tuple[list[Any] | str, dict[str, Any]]:
        """
        Implementation of the log read handler invoked by the Web UI.
        In Airflow 3+, we defer to the super() class so it can serve logs from the active worker
        and intelligently interleave them with `_read_remote_logs`.
        """
        if AIRFLOW_V_3_0_PLUS:
            return super()._read(ti, try_number, metadata)

        # Fallback for Airflow 2.x
        metadata = metadata or {"offset": 0}
        headers, messages = self.io.read("", ti)
        
        # Build raw messages (no StructuredLogMessage required in Airflow 2)
        log_str = "\n".join(messages)
        metadata["end_of_log"] = True 
        
        return [log_str], metadata

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
