# Airflow Translation Agent Skill

This skill defines global terminology and style rules for translating
Apache Airflow UI and documentation. These rules apply to all locales.

## Core Terminology

- Use **Dag** (not DAG) in all translations.
- Keep **XCom** untranslated.
- Keep **ID** untranslated when used as a technical identifier.
- Log Levels must remain in English:
  - CRITICAL
  - ERROR
  - WARNING
  - INFO
  - DEBUG

## General Guidelines

1. **Consistency over literal translation**
   - Prefer established Airflow terminology rather than word-for-word translation.

2. **Do not translate product or feature names**
   - Airflow
   - XCom
   - Dag (use locale transliteration only if standard in that language)

3. **Technical clarity first**
   - If a translation may introduce ambiguity, prefer transliteration or English.

4. **Follow locale-specific skills**
   - Locale files in `locales/` override wording and tone guidance.

## Scope

This skill applies to:

- UI strings
- Documentation snippets
- Generated or automated translations

Locale-specific nuances such as tone, grammar, and preferred wording
must be defined in their respective locale files.
