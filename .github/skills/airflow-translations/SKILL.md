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

- [Airflow Translation Agent Skill](#airflow-translation-agent-skill)
  - [Core Terminology](#core-terminology)
  - [General Guidelines](#general-guidelines)
  - [Scope](#scope)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

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
