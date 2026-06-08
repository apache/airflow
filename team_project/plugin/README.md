# DAG Triage Assistant — Airflow Plugin

An Airflow 3.x plugin that surfaces AI-assisted failure classification
directly in the Task Instance view.

## What it does

When a task fails, the plugin classifies the log into one of five categories
(TRANSIENT, DATA_QUALITY, RESOURCE, CODE, EXTERNAL_DEPENDENCY), shows a
confidence score, and returns actionable remediation steps from a built-in
knowledge base — no LLM calls required for the initial release.

## Installation

### 1. Install the package

```bash
# From this directory (editable install for development):
pip install -e .

# Or install alongside the dag_triage business logic:
pip install -e ../        # installs dag_triage src
pip install -e .          # installs the plugin
```

### 2. Verify Airflow discovers the plugin

```bash
airflow plugins list
```

You should see `dag_triage` in the output.

### 3. Access the triage panel

Start the Airflow API server and open:

```
http://localhost:8080/plugins/dag-triage/
```

The panel also exposes a REST endpoint:

```
POST http://localhost:8080/plugins/dag-triage/api/v1/triage
```

Interactive Swagger docs: `http://localhost:8080/plugins/dag-triage/api/v1/docs`

## Quick demo (no Airflow required)

```bash
python ../../demo.py
```

See `team_project/demo.py` for the standalone walkthrough.

## REST API

### `POST /plugins/dag-triage/api/v1/triage`

**Request body (JSON)**

| Field | Type | Description |
|---|---|---|
| `dag_id` | string | DAG identifier |
| `run_id` | string | DAG run identifier |
| `task_id` | string | Task identifier |
| `log_content` | string | Raw task-instance log text |

**Response**

```json
{
  "dag_id": "etl_pipeline",
  "run_id": "scheduled__2024-01-01T00:00:00+00:00",
  "task_id": "load_customers",
  "failure_category": "CODE",
  "confidence": 0.8,
  "root_cause_summary": "Likely Code failure (confidence 80%). First error: ImportError: No module named 'pandas'",
  "remediations": [
    {
      "title": "Missing Python Dependency",
      "steps": ["Add the missing package to requirements.txt ...", "..."],
      "doc_links": ["https://airflow.apache.org/docs/..."]
    }
  ]
}
```

### `GET /plugins/dag-triage/health`

Returns plugin version and status.

## Development

Run the plugin tests:

```bash
cd team_project/plugin
pytest tests/ -xvs
```

Run the full test suite including business logic:

```bash
cd team_project
pytest tests/ plugin/tests/ -xvs
```
