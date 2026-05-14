# Milestone Mavericks — Team Workspace

## Team

| Name | Role |
|------|------|
| Sohail Anwar | Scrum Master |
| Poorani T S | Team Member |
| Sharan Saravanan | Team Member |

## Course

**CSS 566A — Software Management**
University of Washington Bothell · Spring 2026
Instructor: Prof. Mia Champion

## Project Scope

We are building an **AI-assisted DAG failure triage plugin** for Apache Airflow.
The plugin ingests task-instance logs from a failing DAG run, classifies the
failure using a lightweight heuristic layer followed by an LLM summarization
layer, and surfaces ranked remediation candidates to the on-call engineer.
A local-only execution mode ensures the plugin can run without sending log data
to an external API, satisfying data-residency requirements.

## Links

| Resource | URL |
|----------|-----|
| Fork | https://github.com/break-through-19/airflow |
| Kanban board | https://github.com/users/break-through-19/projects/9 |
