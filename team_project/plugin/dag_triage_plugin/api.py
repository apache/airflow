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
"""FastAPI sub-application for the DAG Triage plugin."""

from __future__ import annotations

import sys
from pathlib import Path

# Allow running outside an installed package (e.g. demo, dev server).
# When the plugin is pip-installed alongside the dag_triage package this
# sys.path manipulation is a no-op.
_SRC = Path(__file__).parents[2] / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from dag_triage.classifier import FailureClassifier  # noqa: E402
from dag_triage.log_parser import parse_log  # noqa: E402
from dag_triage.remediation_kb import RemediationKB  # noqa: E402
from fastapi import FastAPI  # noqa: E402
from fastapi.responses import HTMLResponse  # noqa: E402
from pydantic import BaseModel, Field  # noqa: E402

_PLUGIN_VERSION = "0.1.0"


# ---------------------------------------------------------------------------
# Request / response schemas
# ---------------------------------------------------------------------------


class TriageRequest(BaseModel):
    """Input payload for a triage request."""

    dag_id: str = Field(..., description="DAG identifier")
    run_id: str = Field(..., description="DAG run identifier")
    task_id: str = Field(..., description="Task identifier")
    log_content: str = Field(..., description="Raw task-instance log text")


class RemediationResult(BaseModel):
    """A single remediation entry from the knowledge base."""

    title: str
    steps: list[str]
    doc_links: list[str]


class TriageResponse(BaseModel):
    """Triage result returned for a failed task instance."""

    dag_id: str
    run_id: str
    task_id: str
    failure_category: str | None = Field(
        None,
        description="Top-ranked failure category, or null when no pattern matches",
    )
    confidence: float = Field(0.0, ge=0.0, le=1.0)
    root_cause_summary: str
    remediations: list[RemediationResult]


# ---------------------------------------------------------------------------
# HTML panel served on the Task Instance view
# ---------------------------------------------------------------------------

_PANEL_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>DAG Triage Assistant</title>
  <style>
    body {{ font-family: system-ui, sans-serif; margin: 0; padding: 1rem;
            background: #f9fafb; color: #111827; }}
    h1 {{ font-size: 1.2rem; margin-bottom: .5rem; }}
    #form-section {{ background: #fff; border: 1px solid #e5e7eb;
                     border-radius: .5rem; padding: 1rem; margin-bottom: 1rem; }}
    label {{ display: block; font-size: .85rem; font-weight: 600;
             margin-bottom: .25rem; color: #374151; }}
    input, textarea {{ width: 100%; box-sizing: border-box; padding: .4rem .6rem;
                       border: 1px solid #d1d5db; border-radius: .375rem;
                       font-size: .9rem; margin-bottom: .75rem; }}
    textarea {{ height: 140px; resize: vertical; font-family: monospace;
                font-size: .8rem; }}
    button {{ background: #2563eb; color: #fff; border: none; padding: .5rem 1.25rem;
              border-radius: .375rem; cursor: pointer; font-size: .9rem; }}
    button:hover {{ background: #1d4ed8; }}
    #result {{ display: none; background: #fff; border: 1px solid #e5e7eb;
               border-radius: .5rem; padding: 1rem; }}
    .badge {{ display: inline-block; padding: .2rem .6rem; border-radius: 9999px;
              font-size: .75rem; font-weight: 700; margin-left: .5rem; }}
    .TRANSIENT {{ background:#dbeafe; color:#1e40af; }}
    .DATA_QUALITY {{ background:#fef3c7; color:#92400e; }}
    .RESOURCE {{ background:#fee2e2; color:#991b1b; }}
    .CODE {{ background:#f3e8ff; color:#6b21a8; }}
    .EXTERNAL_DEPENDENCY {{ background:#d1fae5; color:#065f46; }}
    .NONE {{ background:#f3f4f6; color:#6b7280; }}
    h2 {{ font-size: 1rem; margin: 1rem 0 .4rem; }}
    ol {{ margin: 0; padding-left: 1.25rem; }}
    li {{ margin-bottom: .25rem; font-size: .9rem; }}
    a {{ color: #2563eb; font-size: .85rem; }}
    .error-msg {{ color: #dc2626; font-size: .9rem; }}
  </style>
</head>
<body>
  <h1>DAG Triage Assistant</h1>
  <div id="form-section">
    <label>DAG ID</label>
    <input id="dag-id" placeholder="my_dag" />
    <label>Run ID</label>
    <input id="run-id" placeholder="scheduled__2024-01-01T00:00:00+00:00" />
    <label>Task ID</label>
    <input id="task-id" placeholder="my_task" />
    <label>Paste task log</label>
    <textarea id="log-content" placeholder="[2024-01-01T00:00:01.000+0000] {{task.py:142}} ERROR - ImportError: No module named …"></textarea>
    <button onclick="runTriage()">Analyse failure</button>
  </div>
  <div id="result">
    <h2>Category: <span id="cat-label">—</span><span id="cat-badge" class="badge"></span></h2>
    <p id="summary"></p>
    <div id="remediation-block"></div>
  </div>
  <p id="error-block" class="error-msg"></p>

  <script>
    async function runTriage() {{
      const body = {{
        dag_id: document.getElementById("dag-id").value || "unknown",
        run_id: document.getElementById("run-id").value || "unknown",
        task_id: document.getElementById("task-id").value || "unknown",
        log_content: document.getElementById("log-content").value,
      }};
      document.getElementById("error-block").textContent = "";
      document.getElementById("result").style.display = "none";
      try {{
        const resp = await fetch("api/v1/triage", {{
          method: "POST",
          headers: {{ "Content-Type": "application/json" }},
          body: JSON.stringify(body),
        }});
        if (!resp.ok) {{
          const err = await resp.text();
          document.getElementById("error-block").textContent = "Error: " + err;
          return;
        }}
        const data = await resp.json();
        const cat = data.failure_category || "NONE";
        document.getElementById("cat-label").textContent = cat;
        const badge = document.getElementById("cat-badge");
        badge.textContent = (data.confidence * 100).toFixed(0) + "%";
        badge.className = "badge " + cat;
        document.getElementById("summary").textContent = data.root_cause_summary;

        let html = "";
        for (const r of data.remediations) {{
          html += "<h2>" + r.title + "</h2><ol>";
          for (const s of r.steps) html += "<li>" + s + "</li>";
          html += "</ol>";
          if (r.doc_links.length) {{
            html += "<p>" + r.doc_links.map(l => "<a href='" + l + "' target='_blank'>" + l + "</a>").join(" · ") + "</p>";
          }}
        }}
        document.getElementById("remediation-block").innerHTML = html || "<p>No specific remediation found in the knowledge base.</p>";
        document.getElementById("result").style.display = "block";
      }} catch (e) {{
        document.getElementById("error-block").textContent = "Request failed: " + e.message;
      }}
    }}
  </script>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------


def create_app() -> FastAPI:
    """Return a configured FastAPI instance for the DAG Triage plugin."""
    app = FastAPI(
        title="DAG Triage Assistant",
        description="AI-assisted failure classification for Airflow task instances.",
        version=_PLUGIN_VERSION,
        docs_url="/api/v1/docs",
        openapi_url="/api/v1/openapi.json",
    )

    classifier = FailureClassifier()
    kb = RemediationKB()

    @app.get("/health", tags=["meta"])
    def health() -> dict:
        return {"status": "ok", "plugin": "dag_triage", "version": _PLUGIN_VERSION}

    @app.get("/", response_class=HTMLResponse, tags=["panel"], include_in_schema=False)
    def panel() -> str:
        """Serve the self-contained triage panel HTML page."""
        return _PANEL_HTML

    @app.post("/api/v1/triage", response_model=TriageResponse, tags=["triage"])
    def triage(req: TriageRequest) -> TriageResponse:
        """
        Classify a task failure and return ranked remediations.

        Submit the raw log text from any failed Airflow task instance.
        The endpoint returns the top failure category, a confidence score,
        a human-readable root-cause summary, and actionable remediation steps
        drawn from the built-in knowledge base.
        """
        records = parse_log(req.log_content)
        rankings = classifier.classify(req.log_content)

        if not rankings:
            return TriageResponse(
                dag_id=req.dag_id,
                run_id=req.run_id,
                task_id=req.task_id,
                failure_category=None,
                confidence=0.0,
                root_cause_summary=(
                    "No recognizable failure pattern was found in the log. "
                    "Review the raw log for unexpected output or silent errors."
                ),
                remediations=[],
            )

        top_category, confidence = rankings[0]

        error_msgs = [r.message for r in records if r.level in ("ERROR", "CRITICAL")]
        summary_parts = [
            f"Likely {top_category.replace('_', ' ').title()} failure (confidence {confidence:.0%})."
        ]
        if error_msgs:
            summary_parts.append(f"First error: {error_msgs[0][:300]}")
        root_cause_summary = " ".join(summary_parts)

        matched = kb.lookup(top_category, req.log_content)
        # Fall back to category-level lookup when log-signature filter finds nothing.
        if not matched:
            matched = kb.lookup(top_category)

        remediations = [
            RemediationResult(title=r.title, steps=r.steps, doc_links=r.doc_links)
            for r in matched
        ]

        return TriageResponse(
            dag_id=req.dag_id,
            run_id=req.run_id,
            task_id=req.task_id,
            failure_category=top_category,
            confidence=confidence,
            root_cause_summary=root_cause_summary,
            remediations=remediations,
        )

    return app
