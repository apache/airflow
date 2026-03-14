# Agent Skills

Machine-readable contributor workflow skills extracted
automatically from contributing-docs/*.rst files.

## What is a Skill?

A `.. agent-skill::` block embedded in an RST contributing
doc. Skills extend AGENTS.md with executable,
schema-validated, verifiable workflow steps.

## The :context: field

The most critical field. Tells an AI agent whether a
command runs on the HOST or inside BREEZE:

- `host`   — run on your local machine
- `breeze` — run inside `breeze shell`
- `either` — works in both

This directly addresses the warning in AGENTS.md:
"Never run pytest, python, or airflow commands directly
on the host — always use breeze."

## How skills.json is generated

A pre-commit hook (extract-agent-skills) runs automatically
when contributing-docs/*.rst files change. It extracts all
agent-skill blocks and regenerates this file.

Mirrors the update-breeze-cmd-output hook pattern.
