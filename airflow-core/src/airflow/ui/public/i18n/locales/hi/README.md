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

# Apache Airflow के लिए हिन्दी UI अनुवाद

यह दस्तावेज हिन्दी भाषा के लिए चुने गए अनुवाद सिद्धांतों को बताता है। यह दस्तावेज करने के लिए है कि अनुवाद क्यों इस तरह चुने गए और भविष्य के अनुवाद पुनरावृत्तियों में संगतता सुनिश्चित करने के लिए।

## औपचारिक और सम्मानजनक संबोधन

हिन्दी अनुवाद में औपचारिक और सम्मानजनक संबोधन का प्रयोग किया गया है। उपयोगकर्ता को "आप" कहकर संबोधित किया गया है और विनम्र भाषा का प्रयोग किया गया है।

## अपरिवर्तित शब्द

निम्नलिखित शब्दों को जानबूझकर अंग्रेजी से अनुवादित नहीं किया गया है:

- `XCom`: Cross-communication के लिए Airflow का विशिष्ट तकनीकी शब्द है।
- `ID`: Technical identifier के रूप में सभी जगह प्रयोग होता है।
- `Log Levels` (CRITICAL, ERROR, WARNING, INFO, DEBUG): ये तकनीकी स्तर लॉग आउटपुट में भी दिखाई देते हैं, इसलिए इन्हें हिन्दी में अनुवादित नहीं किया गया।

## Airflow-विशिष्ट शब्दों के अनुवाद

हिन्दी अनुवाद के लिए निम्नलिखित शब्दों का इस तरह अनुवाद किया गया है:

- `Asset` → `एसेट`: Airflow 3 में नई तकनीकी अवधारणा होने के कारण और डेटा संपत्ति के साथ भ्रम से बचने के लिए transliteration का प्रयोग किया गया।
- `Asset Event` → `एसेट इवेंट`: तकनीकी संदर्भ में "घटना" के बजाय "इवेंट" अधिक उपयुक्त है।
- `Backfill` → `बैकफ़िल`: तकनीकी शब्द के रूप में रखा गया क्योंकि यह Airflow में विशिष्ट अवधारणा है।
- `Bundle` → `बंडल`: प्रत्यक्ष अनुवाद उपयुक्त है।
- `Catchup` → `पकड़ना`: प्रत्यक्ष अनुवाद।
- `Config`/`Configuration` → `विन्यास`: अंग्रेजी transliteration "कॉन्फ़िगरेशन" के बजाय शुद्ध हिन्दी शब्द का प्रयोग।
- `Connections` → `कनेक्शन`: तकनीकी शब्द के रूप में व्यापक रूप से समझा जाता है।
- `Dag`/`Dags` → `डैग`/`डैग्स`: Airflow का मुख्य concept, transliteration से Hindi speakers के लिए पढ़ना आसान।
- `Dag Run` → `डैग रन`: डैग के execution instance के लिए।
- `Deferred` → `स्थगित`: सबसे उपयुक्त हिन्दी शब्द।
- `Map Index` → `मैप इंडेक्स`: तकनीकी शब्द के रूप में रखा गया।
- `Operator` → `ऑपरेटर`: तकनीकी संदर्भ में व्यापक रूप से समझा जाता है।
- `Plugins` → `प्लगइन`: तकनीकी शब्द।
- `Pools` → `पूल`: संसाधन पूल के संदर्भ में समझा जाता है।
- `Provider` → `प्रोवाइडर`: तकनीकी संदर्भ में transliteration अधिक स्पष्ट है।
- `Scheduled` → `निर्धारित`: नियमित रूप से चलने वाले डैग्स के लिए।
- `Task` → `टास्क`: तकनीकी संदर्भ में व्यापक रूप से समझा जाता है।
- `Task Instance` → `टास्क इंस्टेंस`: तकनीकी शब्द।
- `Trigger` → `ट्रिगर`: तकनीकी संदर्भ में व्यापक रूप से प्रयोग होता है।
- `Try Number` → `प्रयास संख्या`: प्रत्यक्ष अनुवाद।
- `Variable` → `वेरिएबल`: तकनीकी संदर्भ में industry standard transliteration का प्रयोग।
- `Key` → `कुंजी`: डेटाबेस key/identifier के लिए "कुंजी" का प्रयोग, possessive "की" के बजाय।
- `Home` → `मुख्य पृष्ठ`: UI का मुख्य पृष्ठ या डैशबोर्ड के लिए उपयुक्त हिन्दी शब्द, transliteration "होम" के बजाय।

## अनुवाद सिद्धांत

### शुद्ध हिन्दी vs Transliteration

इस अनुवाद में निम्नलिखित सिद्धांत अपनाए गए हैं:

1. **तकनीकी शब्द**: कुछ Airflow-विशिष्ट तकनीकी शब्द (जैसे XCom, ID) अपरिवर्तित रखे गए हैं, जबकि मुख्य concepts (डैग, वेरिएबल) को transliterate किया गया है।
2. **सामान्य UI शब्द**: सामान्य interface शब्दों के लिए शुद्ध हिन्दी शब्द प्राथमिकता (जैसे "विन्यास" बजाय "कॉन्फ़िगरेशन")।
3. **संदर्भ स्पष्टता**: जहां शुद्ध हिन्दी शब्द भ्रम पैदा कर सकते हैं, वहां transliteration का प्रयोग (जैसे "एसेट" बजाय "संपत्ति")।


## विशेष नोट

`states.none` के लिए "कुछ नहीं" का प्रयोग किया गया है, जबकि `states.no_status` के लिए "कोई स्थिति नहीं" का प्रयोग किया गया है, जैसा कि मूल PR में चर्चा हुई थी।

## अनुवाद स्थिति

- **पूर्णता**: 100% (638/638 अनुवाद)
- **फ़ाइलें**: 9 JSON फ़ाइलें (common, admin, browse, assets, components, dag, dags, dashboard, hitl)
- **गुणवत्ता**: सभी linter जांच पास, कोई missing keys नहीं

यह अनुवाद Apache Airflow समुदाय के योगदान से तैयार किया गया है और इसका उद्देश्य हिन्दी भाषी उपयोगकर्ताओं के लिए Airflow को अधिक सुलभ बनाना है।
