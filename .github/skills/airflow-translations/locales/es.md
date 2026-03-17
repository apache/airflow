<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Spanish (es) Translation Agent Skill

**Locale code:** `es`
**Preferred variant:** Neutral international Spanish (es) — inclusive across Latin America, Spain and Equatorial Guinea, consistent with existing translations in `airflow-core/src/airflow/ui/public/i18n/locales/es/`

This file contains locale-specific guidelines so AI translation agents produce new Spanish strings that stay 100% consistent with the existing translations.

## 1. Core Airflow Terminology

### Global Airflow terms (never translate)

These terms are defined as untranslatable across **all** Airflow locales.
Do not translate them regardless of language:

- `Airflow` — Product name
- `Dag` / `Dags` — Airflow concept; never write "DAG"
- `XCom` / `XComs` — Airflow cross-communication mechanism
- `Asset` / `Assets` — Data dependency tracked by Airflow
- `Provider` / `Providers` — Airflow extension package name
- `Map Index` — Task mapping index
- `PID` — Unix process identifier
- `ID` — Universal abbreviation
- `UTC` — Time standard
- `JSON` — Standard technical format name
- `REST API` — Standard technical term
- `Schema` — Database term
- Log levels: `CRITICAL`, `ERROR`, `WARNING`, `INFO`, `DEBUG`

### Kept in English by convention (Spanish-specific)

The existing Spanish locale files leave these terms untranslated.
Keep them in English to stay consistent with established translations:

- `Backfill` / `Backfills` — Airflow-specific retroactive execution concept
- `Catchup` — Airflow scheduling concept
- `Bundle` — Airflow bundle concept
- `Executor` — Airflow component name
- `Plugin` / `Plugins` — Airflow extensibility mechanism
- `Pool` / `Pools` — Resource constraint mechanism
- `Trigger` / `Triggerer` — As **component names and nouns** keep in English (e.g., "Clase del Trigger", "Triggerer Asignado"); as a **verb** translate as "Activar" (see section 2)
- `Upstream` / `Downstream` — Used as-is even within Spanish sentences (e.g., "Fallido en Upstream")
- `Heartbeat` — Used as-is in component health labels (e.g., "Último Heartbeat")

## 2. Standard Translations

The following Airflow-specific terms have established Spanish translations
that **must be used consistently**:

| English Term          | Spanish Translation           | Notes                                         |
| --------------------- | ----------------------------- | --------------------------------------------- |
| Task                  | Tarea                         | Plural: "Tareas"                              |
| Task Instance         | Instancia de Tarea            | Plural: "Instancias de Tarea"                 |
| Task Group            | Grupo de Tareas               |                                               |
| Dag Run               | Ejecución del Dag             | Plural: "Ejecuciones del Dag"                 |
| Trigger (verb)        | Activar                       | "Activado por" for "Triggered by"; as noun/component keep in English (see section 1) |
| Trigger Rule          | Regla de Activación           |                                               |
| Schedule (noun)       | Programación                  |                                               |
| Scheduler             | Programador                   |                                               |
| Operator              | Operador                      | Plural: "Operadores"                          |
| Connection            | Conexión                      | Plural: "Conexiones"                          |
| Variable              | Variable                      |                                               |
| Configuration         | Configuración                 |                                               |
| Audit Log             | Auditoría de Log              | `dag.json` uses this form; `common.json` has "Auditar Log" — prefer "Auditoría de Log" |
| Try Number            | Intento Número                |                                               |
| Timezone              | Zona Horaria                  |                                               |
| Dark Mode             | Modo Oscuro                   |                                               |
| Light Mode            | Modo Claro                    |                                               |
| Tags                  | Etiquetas                     |                                               |
| Owner                 | Propietario                   |                                               |
| Description           | Descripción                   |                                               |
| Duration              | Duración                      |                                               |
| Delete                | Eliminar                      |                                               |
| Cancel                | Cancelar                      |                                               |
| Confirm               | Confirmar                     |                                               |
| Filter (noun/label)   | Filtro                        |                                               |
| Filter (verb)         | Filtrar                       | e.g., "Filtrar Dags por etiqueta"             |
| Reset                 | Restablecer                   |                                               |
| Download              | Descargar                     |                                               |
| Expand / Collapse     | Expandir / Colapsar           |                                               |
| Logout                | Cerrar Sesión                 |                                               |
| Browse                | Navegar                       |                                               |
| Admin                 | Administración                |                                               |
| Security              | Seguridad                     |                                               |
| Users                 | Usuarios                      |                                               |
| Roles                 | Roles                         |                                               |
| Permissions           | Permisos                      |                                               |
| Actions               | Acciones                      |                                               |
| Resources             | Recursos                      |                                               |
| Documentation         | Documentación                 |                                               |
| Home                  | Inicio                        |                                               |

## 3. Task/Run States

| English State       | Spanish Translation   |
| ------------------- | --------------------- |
| running             | En Ejecución          |
| failed              | Fallido               |
| success             | Exitoso               |
| queued              | En Cola               |
| scheduled           | Programado            |
| skipped             | Omitido               |
| deferred            | Diferido              |
| removed             | Removido              |
| restarting          | Reiniciando           |
| up_for_retry        | Por Reintentar        |
| up_for_reschedule   | Por Reprogramar       |
| upstream_failed     | Fallido en Upstream   |
| no_status / none    | Sin Estado            |
| planned             | Planificado           |

## 4. Spanish-Specific Guidelines

### Tone and Register

- Use **neutral, international Spanish** — avoid region-specific idioms so strings work across all Spanish-speaking regions.
- Prefer impersonal constructions over explicit "tú" or "usted" where the existing translations already do so (e.g., "Presiona {{hotkey}} para...").
- Keep UI strings concise — they appear in buttons, labels, and tooltips.

### Gender Agreement

- `Dag` is treated as **masculine**: "el Dag", "Ejecución del Dag"
- `Tarea` is **feminine**: "la tarea", "una tarea"
- `Ejecución` is **feminine**: "la ejecución", "Última Ejecución"
- `Conexión` is **feminine**: "la conexión"
- `Variable` is **feminine**: "la variable"

### Plural Forms

- Spanish uses i18next suffixes `_one` and `_other` only.
  `_many` and `_other` must always use the same translation:

  ```json
  "task_one": "Tarea",
  "task_other": "Tareas"
  ```

### Capitalization

- Use **title case** for UI headers, buttons, and navigation items (e.g., "Todas las Ejecuciones", "Cerrar Sesión").
- Use **sentence case** for descriptions and messages (e.g., "No se encontraron resultados.").

### Technical Loanwords

The following English loanwords are accepted in the existing translations — use them as-is:

- "parsear" (from "parse") — used in "Duración del parseo", "Último Parseado"
- "Wrap" → "Envolver", "Unwrap" → "Desenvolver"

## 5. Examples from Existing Translations

**Always keep in English:**

- "Dag" → "Dag"
- "Asset" → "Asset"
- "XCom" → "XCom"
- "Pool" → "Pool"
- "Backfill" → "Backfill"
- "Catchup" → "Catchup"

**Common translation patterns:**

```
task_one              → "Tarea"
task_other            → "Tareas"
dagRun_one            → "Ejecución del Dag"
dagRun_other          → "Ejecuciones del Dag"
backfill_one          → "Backfill"
backfill_other        → "Backfills"
taskInstance_one      → "Instancia de Tarea"
taskInstance_other    → "Instancias de Tarea"
allRuns               → "Todas las Ejecuciones"
running               → "En Ejecución"
failed                → "Fallido"
success               → "Exitoso"
queued                → "En Cola"
scheduled             → "Programado"
```

**Trigger compound nouns — keep "Trigger"/"Triggerer" in English:**

```
triggerDag.button          → "Trigger"             (UI button label, not translated)
triggerer.class            → "Clase del Trigger"
triggerer.id               → "ID del Trigger"
triggerer.createdAt        → "Tiempo de Creación del Trigger"
triggerer.assigned         → "Triggerer Asignado"
triggerer.latestHeartbeat  → "Último Heartbeat del Triggerer"
triggerer.title            → "Información del Triggerer"
```

**Action verbs (buttons):**

```
Add      → "Agregar"
Delete   → "Eliminar"
Edit     → "Editar"
Save     → "Guardar"
Reset    → "Restablecer"
Cancel   → "Cancelar"
Confirm  → "Confirmar"
Import   → "Importar"
Search   → "Buscar"
Filter   → "Filtrar"
```

**"Cannot X" dialog titles — title-case the key nouns:**

```
Cannot Clear Task Instance  →  "No Se Puede Limpiar la Instancia de Tarea"
```

Note: these titles require a full phrase in Spanish — do not shorten at the expense of meaning.


## 6. Agent Instructions (DO / DON'T)

**DO:**

- Match tone, style, gender agreement, and casing from existing `es/*.json` files
- Use neutral, international Spanish readable across all Spanish-speaking regions
- Preserve all i18next placeholders: `{{count}}`, `{{dagName}}`, `{{hotkey}}`, etc.
- Provide all needed plural suffixes (`_one`, `_other`) for each plural key
- Check existing translations before adding new ones to maintain consistency

**DON'T:**

- Translate Airflow-specific terms listed in section 1
- Use "DAG" — always write "Dag"
- Use informal language, slang, or region-specific expressions
- Invent new vocabulary when an equivalent already exists in the current translations
- Change hotkey values (e.g., `"hotkey": "e"` must stay `"e"`)
- Translate variable names or placeholders inside `{{...}}`

---

**Version:** 1.0 — derived from existing `es/*.json` locale files (February 2026)
