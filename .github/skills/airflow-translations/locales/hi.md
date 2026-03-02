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

- [Hindi Translation Agent Skill](#hindi-translation-agent-skill)
  - [Tone and Style](#tone-and-style)
  - [Keep in English](#keep-in-english)
  - [Preferred Translations](#preferred-translations)
  - [Translation Principles](#translation-principles)
  - [Notes](#notes)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Hindi Translation Agent Skill

This file defines terminology, tone, and translation preferences
for Hindi (hi) translations of Apache Airflow.

## Tone and Style

- Use formal and respectful language.
- Address the user as **"आप"**.
- Prefer clear and simple sentence structure.

## Keep in English

The following terms must remain untranslated:

- XCom
- ID
- Log Levels (CRITICAL, ERROR, WARNING, INFO, DEBUG)

## Preferred Translations

| English Term | Hindi |
|-------------|------|
| Dag | डैग |
| Dag Run | डैग रन |
| Task | कार्य |
| Task Instance | टास्क इंस्टेंस |
| Asset | एसेट |
| Asset Event | एसेट इवेंट |
| Configuration | विन्यास |
| Connections | कनेक्शन |
| Operator | ऑपरेटर |
| Variable | वेरिएबल |
| Plugins | प्लगइन |
| Pools | पूल |
| Provider | प्रोवाइडर |
| Trigger | ट्रिगर |
| Backfill | बैकफ़िल |
| Bundle | बंडल |
| Scheduled | निर्धारित |
| Map Index | मैप इंडेक्स |
| Try Number | प्रयास संख्या |
| Key | कुंजी |
| Home | मुख्य पृष्ठ |

## Translation Principles

1. **Formal UI language**
   - Maintain politeness and clarity suitable for professional software.

2. **Pure Hindi for common UI words**
   - Prefer words like *विन्यास* instead of transliteration where clarity is high.

3. **Transliteration for technical concepts**
   - Use transliteration for Airflow-specific terms (e.g., डैग, टास्क).

4. **Avoid ambiguity**
   - If a literal translation could confuse users, prefer transliteration.

5. **Context awareness**
   - Ensure translations match technical meaning rather than dictionary meaning.

## Notes

- Use **"कुछ नहीं"** for `states.none`.
- Use **"कोई स्थिति नहीं"** for `states.no_status`.

These conventions are derived from the existing Hindi locale guidelines
to ensure consistency across future translations.
