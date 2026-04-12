<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Turkish (tr) Translation Agent Skill

**Locale code:** `tr`
**Preferred variant:** Standard Turkish (tr), consistent with existing translations in `airflow-core/src/airflow/ui/public/i18n/locales/tr/`

This file contains locale-specific guidelines so AI translation agents produce
new Turkish strings that stay 100% consistent with the existing translations.

## 1. Core Airflow Terminology

### Global Airflow terms (never translate)

These terms are defined as untranslatable across **all** Airflow locales.
Do not translate them regardless of language:

- `Airflow` — Product name
- `Dag` / `Dag'ler` — Airflow concept; never write "DAG". Turkish plural uses apostrophe: `Dag'ler`
- `XCom` / `XCom'lar` — Airflow cross-communication mechanism
- `PID` — Unix process identifier
- `ID` — Universal abbreviation
- `UTC` — Time standard
- `JSON` — Standard technical format name
- `REST API` — Standard technical term
- Log levels: `CRITICAL`, `ERROR`, `WARNING`, `INFO`, `DEBUG`

### Translated by convention (Turkish-specific)

Unlike some other locales, the existing Turkish translations translate many Airflow terms
into native Turkish. These established translations **must be used consistently**:

- `Asset` / `Assets` → `Varlık` / `Varlıklar`
- `Backfill` → `Geriye Dönük Çalıştırma`
- `Catchup` → `Yakalama`
- `Plugin` / `Plugins` → `Eklenti` / `Eklentiler`
- `Pool` / `Pools` → `Havuz` / `Havuzlar`
- `Provider` / `Providers` → `Sağlayıcı` / `Sağlayıcılar`
- `Trigger` / `Triggerer` → `Tetikleyici` (noun/component)
- `Executor` → `Yürütücü`

## 2. Standard Translations

The following Airflow-specific terms have established Turkish translations
that **must be used consistently**:

| English Term          | Turkish Translation              | Notes                                          |
| --------------------- | -------------------------------- | ---------------------------------------------- |
| Task                  | Görev                            | Plural: "Görevler"                             |
| Task Instance         | Görev Örneği                     | Plural: "Görev Örnekleri"                      |
| Task Group            | Görev Grubu                      |                                                |
| Dag Run               | Dag Çalıştırması                 | Plural: "Dag Çalıştırmaları"                   |
| Trigger (verb)        | Tetiklemek                       | "Tetiklendi" for "Triggered"                   |
| Trigger Rule          | Tetikleme Kuralı                 |                                                |
| Scheduler             | Zamanlayıcı                      |                                                |
| Schedule (noun)       | Zamanlama                        |                                                |
| Operator              | Operatör                         | Plural: "Operatörler"                          |
| Connection            | Bağlantı                         | Plural: "Bağlantılar"                          |
| Variable              | Değişken                         | Plural: "Değişkenler"                          |
| Configuration         | Yapılandırma                     |                                                |
| Audit Log             | Denetim Günlüğü                  |                                                |
| Log                   | Günlük                           | Plural: "Günlükler"                            |
| State                 | Durum                            |                                                |
| Queue (noun)          | Kuyruk                           | "Kuyrukta" for "queued"                        |
| Duration              | Süre                             |                                                |
| Owner                 | Sahip                            |                                                |
| Tags                  | Etiketler                        |                                                |
| Description           | Açıklama                         |                                                |
| Documentation         | Dokümantasyon                    |                                                |
| Timezone              | Saat Dilimi                      |                                                |
| Dark Mode             | Karanlık Mod                     |                                                |
| Light Mode            | Aydınlık Mod                     |                                                |
| Asset Event           | Varlık Etkinliği                 | Plural: "Varlık Etkinlikleri"                  |
| Dag Processor         | Dag İşlemcisi                    |                                                |
| Try Number            | Deneme Sayısı                    |                                                |
| Heartbeat             | Kalp Atışı                       | e.g., "Son Kalp Atışı" for "Last Heartbeat"   |
| Upstream / Downstream | Yukarı Akış / Aşağı Akış        |                                                |

## 3. Task/Run States

| English State       | Turkish Translation       |
| ------------------- | ------------------------- |
| running             | Çalışıyor                 |
| failed              | Başarısız                 |
| success             | Başarılı                  |
| queued              | Kuyrukta                  |
| scheduled           | Planlanmış                |
| skipped             | Atlanmış                  |
| deferred            | Ertelenmiş                |
| removed             | Kaldırılmış               |
| restarting          | Yeniden Başlatılıyor      |
| up_for_retry        | Yeniden Denenecek         |
| up_for_reschedule   | Yeniden Zamanlanacak      |
| upstream_failed     | Yukarı Akış Başarısız     |
| no_status / none    | Durum Yok                 |
| planned             | Planlanmış                |

## 4. Turkish-Specific Guidelines

### Tone and Register

- Use a **neutral, formal Turkish** tone suitable for technical software UIs.
- Use polite imperative forms for instructions (e.g., "tuşuna basın" for "press key").
- Keep UI strings concise — they appear in buttons, labels, and tooltips.

### Vowel Harmony and Suffixes

- Turkish uses vowel harmony for suffixes. Match suffixes to the final vowel of the root word:
  - "Dag'ler" (not "Dag'lar") — follows front vowel harmony
  - "Bağlantılar" — follows back vowel harmony
- Use apostrophe before suffixes on proper nouns and abbreviations: `Dag'ler`, `XCom'lar`

### Plural Forms

- Turkish uses i18next plural suffixes `_one` and `_other` only.
  There is no `_many` or `_zero` in the existing translations:

  ```json
  "task_one": "Görev",
  "task_other": "Görevler"
  ```

  ```json
  "dagRun_one": "Dag Çalıştırması",
  "dagRun_other": "Dag Çalıştırmaları"
  ```

### Capitalization

- Use **title case** for UI headers, navigation items, and button labels (e.g., "Tüm Çalıştırmalar", "Genel Bakış").
- Use **sentence case** for descriptions and longer messages.
- Capitalize the first letter of each major word in compound terms: "Dag Çalıştırması", "Görev Örneği".

### Possessive Constructions

- Turkish uses possessive suffixes for compound nouns:
  - "Dag Çalıştırması" (Dag's run) — possessive -ı/i suffix
  - "Görev Grubu" (Task's group) — possessive -u/ü suffix
  - "Bağlantı Kimliği" (Connection's ID) — possessive -ı/i suffix

## 5. Examples from Existing Translations

**Terms translated to Turkish (unlike some other locales):**

```
Asset          → "Varlık"
Backfill       → "Geriye Dönük Çalıştırma"
Pool           → "Havuz"
Plugin         → "Eklenti"
Provider       → "Sağlayıcı"
Executor       → "Yürütücü"
Trigger        → "Tetikleyici"
Heartbeat      → "Kalp Atışı"
Upstream       → "Yukarı Akış"
Downstream     → "Aşağı Akış"
```

**Common translation patterns:**

```
task_one              → "Görev"
task_other            → "Görevler"
dagRun_one            → "Dag Çalıştırması"
dagRun_other          → "Dag Çalıştırmaları"
backfill_one          → "Geriye Dönük Çalıştırma"
backfill_other        → "Geriye Dönük Çalıştırmalar"
taskInstance_one      → "Görev Örneği"
taskInstance_other    → "Görev Örnekleri"
allRuns               → "Tüm Çalıştırmalar"
running               → "Çalışıyor"
failed                → "Başarısız"
success               → "Başarılı"
queued                → "Kuyrukta"
scheduled             → "Planlanmış"
```

**Trigger compound nouns — translated to Turkish:**

```
triggerer.class           → "Tetikleyici sınıfı"
triggerer.id              → "Tetikleyici Kimliği"
triggerer.createdAt       → "Tetikleyici oluşturma zamanı"
triggerer.assigned        → "Atanmış tetikleyici"
triggerer.latestHeartbeat → "En son tetikleyici kalp atışı"
triggerer.title           → "Tetikleyici Bilgisi"
```

**Action verbs (buttons):**

```
Add      → "Ekle"
Delete   → "Sil"
Edit     → "Düzenle" / "Güncelle"
Save     → "Kaydet"
Reset    → "Sıfırla"
Cancel   → "İptal"
Confirm  → "Onayla"
Import   → "İçe Aktar"
Search   → "Ara"
Filter   → "Filtrele"
Download → "İndir"
Expand   → "Genişlet"
Collapse → "Daralt"
```

**Health/status labels:**

```
Healthy   → "Sağlıklı"
Unhealthy → "Sağlıksız"
```

## 6. Agent Instructions (DO / DON'T)

**DO:**

- Match tone, style, vowel harmony, and casing from existing `tr/*.json` files
- Use formal, neutral Turkish suitable for professional UIs
- Preserve all i18next placeholders: `{{count}}`, `{{dagName}}`, `{{hotkey}}`, etc.
- Apply correct Turkish vowel harmony for suffixes
- Use apostrophe before suffixes on proper nouns and abbreviations (Dag'ler, XCom'lar)
- Provide all needed plural suffixes (`_one`, `_other`) for each plural key
- Check existing translations before adding new ones to maintain consistency

**DON'T:**

- Write "DAG" — always write "Dag"
- Use informal or colloquial Turkish
- Leave terms in English when the existing Turkish translations have established Turkish equivalents (unlike other locales, Turkish translates most terms)
- Invent new vocabulary when an equivalent already exists in the current translations
- Change hotkey values (e.g., `"hotkey": "e"` must stay `"e"`)
- Translate variable names or placeholders inside `{{...}}`

---

**Version:** 1.0 — derived from existing `tr/*.json` locale files (April 2026)
