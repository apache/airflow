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

import asyncio
import contextlib
import dataclasses
import logging
import sys
import threading
import time
import traceback
from collections import deque
from enum import Enum
from time import perf_counter
from types import FrameType
from typing import TYPE_CHECKING

import structlog

from airflow._shared.timezones import timezone
from airflow.stats import Stats

if TYPE_CHECKING:
    from structlog.typing import FilteringBoundLogger

logger = logging.getLogger(__name__)


log: FilteringBoundLogger = structlog.get_logger(logger_name=__name__)


class Phase(str, Enum):
    """Phase of stall detection."""

    START = "start"
    UPDATE = "update"


@dataclasses.dataclass
class StallSample:
    """Single stack capture during a stall."""

    taken_at_perf: float
    stack_text: str


@dataclasses.dataclass
class StallIncident:
    """A coalesced stall incident with start/end, and samples."""

    started_at_perf: float
    ended_at_perf: float | None
    threshold: float
    samples: list[StallSample]
    # Human-readable timestamps for convenience
    started_at_utc: str = dataclasses.field(default="")
    ended_at_utc: str = dataclasses.field(default="")

    def duration(self) -> float | None:
        return None if self.ended_at_perf is None else (self.ended_at_perf - self.started_at_perf)


def _utc_now_str() -> str:
    return timezone.utcnow().isoformat(timespec="seconds")


def _format_stack_from_frame(frame: FrameType, max_frames: int) -> str:
    # Limit frames from the BOTTOM (most recent last) to keep prints short.
    limit: int | None = max_frames if max_frames > 0 else None
    return "".join(traceback.format_stack(frame, limit=limit))


class AsyncioStallMonitor:
    """
    Hybrid stall monitor.

    Detection path:
      - In-loop heartbeat updates a shared timestamp.
      - Background thread checks wall time vs. last heartbeat; if exceeds threshold,
        we enter "stall" state, sample loop thread stack, and log.
      - While stalled, we periodically re-sample (min_report_interval).
      - When the loop catches up (heartbeat moves), we close the incident and
        correlate with asyncio tasks inside the loop thread.
    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        *,
        threshold: float = 0.2,  # seconds without heartbeat => stall
        heartbeat_interval: float = 0.1,  # loop posts heartbeat this often
        min_report_interval: float = 0.5,  # during a long stall, log at most this often
        max_frames: int = 25,  # limit stack frames captured
        history_size: int = 1,  # TODO: increase and make history more useful in the future
    ) -> None:
        self.loop = loop
        self.threshold = float(threshold)
        self.heartbeat_interval = float(heartbeat_interval)
        self.min_report_interval = float(min_report_interval)
        self.max_frames = int(max_frames)

        self._hb_perf: float = perf_counter()
        self._hb_handle: asyncio.Handle | None = None
        self._running = False
        self._thread: threading.Thread | None = None
        self._loop_thread_ident: int | None = None

        self._incident: StallIncident | None = None
        self._last_report_perf: float = 0.0

        self._lock = threading.Lock()
        self.history: deque[StallIncident] = deque(maxlen=history_size)

    def start(self) -> None:
        if self._running:
            return
        self._running = True

        def _capture_thread_ident():
            self._loop_thread_ident = threading.get_ident()

        self.loop.call_soon(_capture_thread_ident)

        # Kick off the heartbeat loop (keep the handle so we can cancel later)
        self._hb_handle = self.loop.call_soon(self._heartbeat)

        # Start watchdog thread
        self._thread = threading.Thread(target=self._watchdog, name="asyncio-stall-watchdog", daemon=True)
        self._thread.start()
        log.info(
            "AsyncioStallMonitor started: threshold=%.3fs heartbeat=%.3fs",
            self.threshold,
            self.heartbeat_interval,
        )

    def stop(self) -> None:
        self._running = False
        log.info("AsyncioStallMonitor stopping...")
        # Cancel any scheduled heartbeat callback to avoid stray timers
        handle = self._hb_handle
        self._hb_handle = None
        if handle is not None:
            with contextlib.suppress(Exception):
                handle.cancel()
        if self._incident is not None:
            self._incident.ended_at_perf = perf_counter()
            self._incident.ended_at_utc = _utc_now_str()
            incident = self._incident
            self._incident = None
            with self._lock:
                self.history.append(incident)
            log.warning(
                "Event loop stall ended (monitor stop). Duration: %.3fs.", incident.duration() or -1.0
            )
        # Join the watchdog so we know it's really gone
        t = self._thread
        self._thread = None
        if t and t.is_alive():
            t.join(timeout=self.heartbeat_interval * 2)

        log.info("AsyncioStallMonitor stopped.")

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.stop()

    def _heartbeat(self) -> None:
        if not self._running:
            return
        self._hb_perf = perf_counter()
        # Reschedule next beat
        self.loop.call_later(self.heartbeat_interval, self._heartbeat)

    def _watchdog(self) -> None:
        while self._running:
            time.sleep(self.heartbeat_interval)
            now = perf_counter()
            gap = now - self._hb_perf

            if gap > self.threshold:
                # We are in a stall
                if self._incident is None:
                    # Stall START
                    self._incident = StallIncident(
                        started_at_perf=now - gap,  # best estimate: when heartbeat stopped advancing
                        ended_at_perf=None,
                        threshold=self.threshold,
                        samples=[],
                        started_at_utc=_utc_now_str(),
                        ended_at_utc="",
                    )
                    self._last_report_perf = 0.0
                    self._sample_and_log(now, phase=Phase.START)

                # During stall: periodic updates
                elif now - self._last_report_perf >= self.min_report_interval:
                    self._sample_and_log(now, phase=Phase.UPDATE)

            else:
                # No stall *or* stall ended: close & correlate if needed
                if self._incident is not None:
                    self._incident.ended_at_perf = now
                    self._incident.ended_at_utc = _utc_now_str()
                    incident = self._incident
                    self._incident = None
                    self._post_incident_store(incident)

    def _sample_and_log(self, now_perf: float, *, phase: str) -> None:
        stack_text = self._capture_loop_stack_bounded()
        if stack_text is None:
            stack_text = "<loop thread stack unavailable>"

        sample = StallSample(taken_at_perf=now_perf, stack_text=stack_text)
        if self._incident:
            self._incident.samples.append(sample)

        if phase == Phase.START:
            log.warning("Event loop stall detected (gap≥%.3fs). Captured loop stack.", self.threshold)
        else:
            log.warning(
                "Event loop still stalled (%.3fs since start).",
                now_perf - (self._incident.started_at_perf if self._incident else now_perf),
            )
        log.warning("%s", stack_text)
        Stats.incr("triggers.blocked_main_thread")

        self._last_report_perf = now_perf

    def _capture_loop_stack_bounded(self) -> str | None:
        ident = self._loop_thread_ident
        if ident is None:
            return None
        try:
            frame = sys._current_frames().get(ident)
            if not frame:
                return None
            return _format_stack_from_frame(frame, self.max_frames)
        except Exception:
            return None

    def _post_incident_store(self, incident: StallIncident) -> None:
        """Schedule incident store and log a summary."""

        def log_and_store():
            with self._lock:
                self.history.append(incident)

            dur = incident.duration()
            log.warning(
                "Event loop stall ended. Duration: %.3fs.",
                dur if dur is not None else -1.0,
            )

        # Use thread-safe scheduling since we're in a background thread
        with contextlib.suppress(RuntimeError):
            self.loop.call_soon_threadsafe(log_and_store)
