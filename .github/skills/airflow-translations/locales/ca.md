<!-- SPDX-License-Identifier: Apache-2.0
https://www.apache.org/licenses/LICENSE-2.0 -->

# Catalan (ca) Translation Guidelines

This document provides basic guidelines for translating Apache Airflow UI content into Catalan (ca).  
The goal is to maintain clarity, consistency, and alignment with existing Airflow terminology.

## General Principles

- Some core technical terms such as Dag or Xcom should remain in English unless otherwise noted in this skill.
- Avoid translating class names, identifiers, or code-related terminology.
- Keep translations clear and easy to understand for end users.

## Consistency

- Follow the same terminology across the entire UI.
- If a term is already translated elsewhere in the project, reuse that translation.
- When unsure, prioritize consistency over literal translation.

## Style

- Use natural, user-friendly phrasing.
- Prefer clarity over word-by-word translation.
- Avoid overly complex sentence structures.

## Notes

If there is ambiguity in translation, check existing locale files for reference  
or align with the terminology used in official Airflow documentation.

## Airflow-Specific Notes

- Keep terminology consistent with existing Airflow documentation.
- Do not translate DAG-related identifiers such as `dag_id`, `task_id`, or parameter names.
- When referring to Airflow components (Scheduler, Webserver, Executor), ensure naming matches official documentation.
- UI labels should remain concise and consistent with other locale files.