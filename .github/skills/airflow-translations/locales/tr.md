<!-- SPDX-License-Identifier: Apache-2.0 https://www.apache.org/licenses/LICENSE-2.0 -->
# Turkish (tr)

This document provides locale-specific instructions for translating English
Airflow UI strings into Turkish. It inherits all global rules from
the parent [SKILL.md](../SKILL.md).

## Plural Forms

Turkish **does not** use plural forms after numbers. When a number precedes a
noun, the noun remains in singular form:

**English source:**

```json
"dagRun_one": "Dag Run",
"dagRun_other": "Dag Runs"
```

**Correct** — singular form for both:

```json
"dagRun_one": "Dag Çalıştırması",
"dagRun_other": "Dag Çalıştırması"
```

When there is no number, plural suffix `-ler/-lar` may be used contextually,
but for consistency with the number-based pattern, use the same translation
for both `_one` and `_other` suffixes.

## Spacing Rules

Use **standard spacing** rules:

- Single space between words
- No special spacing rules for numbers or English terms mixed with Turkish

**Correct:**

```json
"Dag çalıştırması"
"Son 12 saat"
"Bağlantı ID"
"{{count}} bağlantı"
```

## Punctuation

- Use **standard Turkish punctuation**: `,` `.` `:` `?` `!`
- Turkish uses the same punctuation marks as English
- Use standard parentheses: `(` `)`

**Correct:**

```json
"confirmation": "{{resourceName}} öğesini silmek istediğinizden emin misiniz? Bu işlem geri alınamaz."
```

## Tone and Formality

- Use **formal register** with "Siz" (formal "you") for all user interactions
- Use imperative forms respectfully: "Lütfen seçiniz" (Please select)
- Keep translations **concise** — these are UI labels and button text
- Avoid overly casual or colloquial expressions

**Examples:**

```json
"confirmation": "Silmek istediğinizden emin misiniz?"  // Using "Siz" form
"action": "Lütfen bir seçenek belirleyin"              // Polite imperative
```

## Case and Capitalization

- Turkish has specific capitalization rules:
  - Capital `İ` and lowercase `i` (dotted)
  - Capital `I` and lowercase `ı` (dotless)
- Use **sentence case** for most UI elements (capitalize only the first word)
- Use **title case** only for major headings

**Correct:**

```json
"title": "Dag çalıştırması"           // Sentence case
"button": "Bağlantıyı sil"            // Sentence case
"heading": "Bağlantı Yönetimi"        // Title case for major heading
```

## Variable and Placeholder Examples

Preserve all `{{variable}}` placeholders. Adjust word order as needed for
natural Turkish syntax:

**English source:**

```json
"title": "Mark {{type}} as {{state}}"
```

**Correct** — placeholders preserved, natural Turkish word order:

```json
"title": "{{type}} öğesini {{state}} olarak işaretle"
```

**Incorrect** — variable names translated:

```json
"title": "{{tür}} öğesini {{durum}} olarak işaretle"
```

## Terminology Reference

The following translations should be used consistently for common Airflow terms.
Before translating, **check existing tr locale files** (when available) to
maintain consistency:

| English Term | Turkish Translation | Notes |
|---|---|---|
| Task | Görev | |
| Task Instance | Görev Örneği | |
| Task Group | Görev Grubu | |
| Dag | Dag | Keep as is (product term) |
| Dag Run | Dag Çalıştırması | |
| Operator | Operatör | |
| Trigger | Tetikleyici | |
| Trigger Rule | Tetikleme Kuralı | |
| Triggerer | Tetikleyici | Same as "Trigger" in Turkish |
| Schedule | Zamanlama | |
| Scheduler | Zamanlayıcı | |
| Backfill | Geçmiş Veri Doldurma | |
| Asset | Varlık | |
| Asset Event | Varlık Olayı | |
| Connection | Bağlantı | |
| Variable | Değişken | |
| Pool | Havuz | |
| Plugin | Eklenti | |
| Executor | Yürütücü | |
| Queue | Kuyruk | |
| Audit Log | Denetim Günlüğü | |
| Provider | Sağlayıcı | |
| XCom | XCom | Keep as is (product term) |
| Run | Çalıştırma | As noun |
| Execute | Çalıştır | As verb |
| Delete | Sil | |
| Edit | Düzenle | |
| Create | Oluştur | |
| Update | Güncelle | |
| Refresh | Yenile | |
| Filter | Filtrele | |
| Search | Ara | |
| Sort | Sırala | |
| View | Görüntüle | |
| Details | Ayrıntılar | |
| Settings | Ayarlar | |
| Configuration | Yapılandırma | |
| Status | Durum | |
| State | Durum | Same as Status |
| Success | Başarılı | |
| Failed | Başarısız | |
| Running | Çalışıyor | |
| Queued | Kuyrukta | |
| Scheduled | Zamanlanmış | |

## Common Phrases

| English | Turkish |
|---|---|
| Are you sure? | Emin misiniz? |
| This action cannot be undone | Bu işlem geri alınamaz |
| Please confirm | Lütfen onaylayın |
| Loading... | Yükleniyor... |
| No data available | Veri yok |
| Select all | Tümünü seç |
| Clear all | Tümünü temizle |
| Apply | Uygula |
| Cancel | İptal |
| Save | Kaydet |
| Close | Kapat |
| Back | Geri |
| Next | İleri |
| Previous | Önceki |
| Show more | Daha fazla göster |
| Show less | Daha az göster |
| Expand | Genişlet |
| Collapse | Daralt |

## Translation Workflow

1. **Read** this guideline document thoroughly
2. **Check** existing Turkish translations in:
   ```
   airflow-core/src/airflow/ui/public/i18n/locales/tr/
   ```
3. **Maintain consistency** with established terminology
4. **Use formal tone** throughout
5. **Preserve** all `{{variables}}` exactly as they appear
6. **Keep it concise** — UI space is limited
