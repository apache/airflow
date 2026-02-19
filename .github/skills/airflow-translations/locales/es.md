<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Spanish Translation Agent Skill](#spanish-translation-agent-skill)
  - [Tone and Style](#tone-and-style)
  - [Keep in English](#keep-in-english)
  - [Preferred Translations](#preferred-translations)
  - [Translation Principles](#translation-principles)
  - [Agent Instructions (DO / DON'T)](#agent-instructions-do--dont)
  - [Notes](#notes)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Spanish Translation Agent Skill

**Locale code:** `es`

This file defines terminology, tone, and translation preferences
for Spanish (es) translations of Apache Airflow.

It is derived from the existing Spanish locale files at:

`airflow-core/src/airflow/ui/public/i18n/locales/es/*.json`

## Tone and Style

- Use **neutral, formal Spanish** suitable for professional software documentation.
- Avoid region-specific idioms; prefer standard/international Spanish understood across Latin America and Spain.
- Address the user implicitly (impersonal constructions) rather than "tú" or "usted" when possible — this is the pattern already established in the existing translations (e.g., "Presiona {{hotkey}} para..." instead of "Presione usted...").
- Use polite, clear, and concise language.

## Keep in English

The following terms must remain untranslated:

- **Dag / Dags** — Airflow's core concept; always "Dag" (never "DAG")
- **Asset / Assets** — kept as-is in the existing translations
- **Backfill / Backfills** — Airflow-specific concept
- **XCom / XComs** — cross-communication mechanism
- **Pool / Pools** — resource pool concept
- **Plugins** — kept as-is
- **ID** — universal technical identifier
- **Log levels** (CRITICAL, ERROR, WARNING, INFO, DEBUG) — match log output
- **Upstream / Downstream** — when referring to task dependencies (used as "Fallido en Upstream")
- **Executor** — Airflow component name
- **Trigger / Triggerer** — Airflow component names (but the verb "trigger" is translated as "Activar")
- **Bundle** — Airflow bundle concept

## Preferred Translations

| English Term | Spanish | Notes |
|---|---|---|
| Dag Run | Ejecución del Dag | |
| Task | Tarea | |
| Task Instance | Instancia de Tarea | |
| Operator | Operador | |
| Connections | Conexiones | |
| Variables | Variables | |
| Providers | Proveedores | |
| Configuration | Configuración | |
| Schedule / Scheduled | Programación / Programado | |
| Running | En Ejecución | |
| Queued | En Cola | |
| Failed | Fallido | |
| Success / Successful | Exitoso | |
| Deferred | Diferido | |
| Skipped | Omitido | |
| Removed | Removido | |
| Restarting | Reiniciando | |
| Planned | Planificado | |
| Tags | Etiquetas | |
| Owner | Propietario | |
| Description | Descripción | |
| Duration | Duración | |
| Delete | Eliminar | |
| Cancel | Cancelar | |
| Confirm | Confirmar | |
| Filter | Filtro | |
| Reset | Restablecer | |
| Download | Descargar | |
| Expand / Collapse | Expandir / Colapsar | |
| Logout | Cerrar Sesión | |
| Browse | Navegar | |
| Admin | Administración | |
| Security | Seguridad | |
| Users | Usuarios | |
| Roles | Roles | |
| Permissions | Permisos | |
| Actions | Acciones | |
| Resources | Recursos | |
| Documentation | Documentación | |
| Home | Inicio | |
| Trigger (verb) | Activar | "Activado por" for "Triggered by" |
| Trigger Rule | Regla de Activación | |
| Catchup | Catchup | Kept in English |
| Try Number | Intento Número | |
| Map Index | Mapa de Índice | |
| Timezone | Zona Horaria | |
| Dark Mode | Modo Oscuro | |
| Light Mode | Modo Claro | |
| Audit Log | Auditar Log | |

## Translation Principles

1. **Neutral Spanish**
   - Use international/neutral Spanish that works across all Spanish-speaking regions.
   - Avoid regionalisms (e.g., prefer "Computadora" over "Ordenador" when relevant).

2. **Airflow terms stay in English**
   - Core Airflow concepts (Dag, Asset, XCom, Pool, Backfill) are kept in English.
   - This ensures consistency with code, logs, and documentation.

3. **Natural capitalization**
   - Use title case for UI headers, buttons, and navigation items (e.g., "Todas las Ejecuciones").
   - Use sentence case for descriptions and messages (e.g., "No se encontraron resultados").

4. **Preserve placeholders and formatting**
   - All `{{variable}}` placeholders must be preserved exactly.
   - Adjust surrounding grammar to fit Spanish word order around placeholders.

5. **Plural forms**
   - Follow i18next plural keys: `_one`, `_many`, `_other`.
   - Spanish uses `_one` for singular and `_other` for plural (same form as `_many`).

6. **Gender agreement**
   - Maintain correct grammatical gender (e.g., "Ejecución" is feminine → "Última Ejecución").
   - "Dag" is treated as masculine in Spanish (e.g., "Ejecución del Dag").

## Agent Instructions (DO / DON'T)

**DO:**
- Match tone, style, gender, and casing from existing `es/*.json` files
- Use clear, professional Spanish readable across all Spanish-speaking regions
- Preserve all placeholders: `{{count}}`, `{{dagName}}`, `{{hotkey}}`, etc.
- For plurals: provide all needed suffixes (`_one`, `_many`, `_other`)
- Check existing translations before adding new ones to maintain consistency

**DON'T:**
- Translate core Airflow terms listed in the "Keep in English" section
- Use "DAG" — always use "Dag"
- Use informal language or slang
- Invent new vocabulary when an equivalent already exists in the JSON files
- Translate hotkeys, code references, variable names, or file paths
- Use region-specific expressions that could confuse speakers from other regions

## Notes

- `states.none` and `states.no_status` are both translated as **"Sin Estado"** in the existing files.
- The verb "parsear" (from English "parse") is used as an accepted technical loanword (e.g., "Duración del parseo", "Último Parseado").
- "Wrap" is translated as **"Envolver"** and "Unwrap" as **"Desenvolver"**.
- Asset-related terms keep "Asset" in English (e.g., "Evento de Asset", "Eventos de Asset Fuente").

---

**Version:** 1.0 — based directly on current `es/` JSON files (Feb 2026)
