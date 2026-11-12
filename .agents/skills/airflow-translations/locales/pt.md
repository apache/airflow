<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Portuguese (pt) Translation Agent Skill

**Locale code:** `pt`
**Preferred variant:** Mix of pt-BR / pt-PT as visible in existing files (Brazilian spelling often preferred, but some European forms like "Secção" appear)

This file contains locale-specific guidelines so AI translation agents produce new Portuguese strings that stay 100% consistent with the existing translations in:

`airflow-core/src/airflow/ui/public/i18n/locales/pt/*.json`

## 1. Core Airflow Terminology

- Keep these terms **in English unchanged** (case-sensitive):
  - Dag / Dags
  - Asset / Assets / Asset Events
  - Backfill / Backfills
  - XCom / XComs
  - TaskInstance, DagRun, Triggerer, Executor, Pool, Provider, etc.
- Never use "DAG" – always "Dag"
- Product names: "Airflow" stays "Airflow"

## 2. Portuguese-Specific Guidelines

- Use natural, fluent Portuguese.
- Respect gender and number agreement (feminine/masculine, singular/plural).
- Follow existing i18next plural keys: `_one`, `_many`, `_other`, `_zero`
- Capitalization: Sentence case for descriptions, title-like for headers/buttons (match existing files).

### Plurals and "de"

Portuguese needs `_zero`, `_one`, `_many` and `_other`. A key that omits `_many` falls through to the English `fallbackLng` rather than to the Portuguese `_other`.

`_many` resolves only at exact multiples of a million (1.000.000, 2.000.000, …). 1.500.000 and 1.000.001 resolve to `_other`. `_zero` is the explicit zero-count message (`Nenhum` / `Nenhuma`).

At those million counts a numeral takes **de** before the noun it quantifies ("um milhão **de** tarefas"), so a `_many` form that merely repeats `_other` reads ungrammatically when the string interpolates a count directly in front of a noun:

```json
"change_one": "{{count, number}} alteração",
"change_many": "{{count, number}} de alterações",
"change_other": "{{count, number}} alterações",
"change_zero": "Nenhuma alteração"
```

When the count stands in brackets and does not quantify the following noun, **de** does not apply and `_many` matches `_other`:

```json
"requiredActionCount_many": "Ações Necessárias ({{count, number}})"
```

`outros` is still a quantified word, so `teams.more_many` is `e {{count, number}} de outros`. A bare noun with no numeral in the string is unaffected (`task_many` stays `Tarefas`). The UI prints the count as digits (`1.000.000`), not as the words "um milhão". Keep `{{count, number}}` anyway: that is the form i18next selects `_many` for.

A saved filter is `filtro salvo`. `padrão` is only the default (`Definir como padrão`). A deadline is `Prazo`. A loop is `Ciclo`; one pass of it is `Iteração`. A Dag bundle is `Pacote do Dag`, matching the existing "versão mais recente do pacote".

- Spelling/vocab: Follow patterns in current JSON (e.g. "Excluir", "Adicionar", "Executando", "Enfileirado", "Secção", "detetadas", "Sobreescreve").

## 3. Examples from Existing Translations

**Always keep in English:**

- "Dag" → "Dag"
- "Asset" → "Asset"
- "Backfill" → "Backfill"
- "XCom" → "XCom"

**Common translated patterns:**

- "task_one"       → "Tarefa"
- "task_many"      → "Tarefas"
- "dagRun_one"     → "Execução do Dag"
- "dagRun_many"    → "Execuções do Dag"
- "allRuns"        → "Todas as Execuções"
- "running"        → "Executando"
- "failed"         → "Falha"
- "success"        → "Sucesso"
- "queued"         → "Enfileirado"
- "Add"            → "Adicionar"
- "Delete"         → "Excluir"
- "Edit"           → "Editar"
- "Save"           → "Salvar"
- "Test"           → "Testar"
- "Import"         → "Importar"
- "Config"         → "Configuração do Airflow"

## 4. Agent Instructions (DO / DON'T)

**DO:**

- Match tone, style, gender, casing from existing `pt/*.json` files
- Use natural Portuguese readable by Brazilian & Portuguese users
- Preserve all placeholders: `{{count, number}}`, `{{dagName}}`, etc.
- For plurals: provide all needed suffixes if source has them

**DON'T:**

- Translate core terms listed in section 1
- Use inconsistent gender (e.g. "Execução de Dag" instead of "Execução do Dag")
- Invent new vocabulary when equivalent already exists
- Translate hotkeys, code references, or file paths

---

**Version:** 1.0 – based directly on current `pt/` JSON files (Feb 2026)
