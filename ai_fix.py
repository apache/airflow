# AI-generated fix (fallback):
```diff
PR: Define translation agent skill guidelines for Hebrew (he) locale
Closes #61993

diff --git a/airflow/locale/he/LC_MESSAGES/agent_skills.po b/airflow/locale/he/LC_MESSAGES/agent_skills.po
new file mode 100644
index 0000000..8b96724
--- /dev/null
+++ b/airflow/locale/he/LC_MESSAGES/agent_skills.po
@@ -0,0 +1,5 @@
+msgid "translation_agent_skill_guidelines"
+msgstr "הנחיות לתרגום סיכות סוכן"
+
+msgid "hebrew_translation_guidelines"
+msgstr "הנחיות תרגום לעברית"

diff --git a/airflow/translations.py b/airflow/translations.py
index 8fbafe2..9b4e3c4 100644
--- a/airflow/translations.py
+++ b/airflow/translations.py
@@ -10,6 +10,7 @@ TRANSLATION_AGENT_SKILL_GUIDELINES = {
     'en': 'Translation Agent Skill Guidelines',
     'fr': 'Lignes directrices des compétences de l\'agent de traduction',
+    'he': 'הנחיות לתרגום סיכות סוכן',
 }
```
