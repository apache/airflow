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

# Deutsche UI Übersetzung für Apache Airflow

Dieses Dokument beschreibt die Grundsätze der Übersetzung, die für die Deutsche
Sprache gewählt wurde. Es soll dokumentieren warum die Übersetzungen so gewählt
wurden und damit neben einer Begründung eine möglichst einheitliche Konsistenz
zukünftiger Übersetzungsiterationen ermöglichen.

## Neutrale und formelle Anrede

Im Deutschen wird im Vergleich zu der Englischen Sprache die förmliche- von der
normalen Anrede unterschieden
([Siehe: Wikipedia](https://de.wikipedia.org/wiki/Anrede)). Für die Deutsche
Übersetzung und unter der Annahme des "Nicht-Wissens" des Benutzerkreises wurde
die förmliche Anrede verwendet.

## Feststehende Terme

Die folgenden Begriffe wurden bewusst nicht aus dem Englischen übersetzt:

- Dag / Dags: Nach der Diskussion in der Devlist in
  (["Airflow should deprecate the term "DAG" for end users"](https://lists.apache.org/thread/lktrzqkzrpvc1cyctxz7zxfmc0fwtq2j))
  und der globalen Umbenennung aller Nutzung von `DAG` zu `Dag` als neuem
  feststehenden Begriff in
  ([[LAZY CONSENSUS] rename DAGs to dags / Dags in docs](https://lists.apache.org/thread/24hs06s39l73gj2h4o8l5dr2czgg2gw0))
  ist es sinnvoll diesen Begriff als markenähnlichen Begriff in Airflow
  konsistent mit der weit verbreiteten Verwendung des Begriffs in der Dokumentation zu behalten. Die deutsche Übersetzung als
  "Workflow" wäre vermutlich eher irreführend und es ist anzunehmen dass die
  Nutzer von Airflow den Begriff zuordnen können.
  Der Begriff `Dag` wird in der deutschen Übersetzung im Neutrum verwendet.
- Log level "CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG" in dag.json
  Abschnitt "logs": Diese Begriffe werden in den feststehenden Logs im Text
  auch ausgegeben, deswegen werden sie nicht in das Deutsche übertragen.

(Derzeit keine weiteren feststehenden Begriffe)

## Definitionen von Übersetzungen von Airflow-Spezifischen Termen

Für die Deutsche Übersetzung wurden die folgenden Terme wie folgt übersetzt
(in alphabethischer Reihenfolge):

- `Asset`/`Assets` --> `Datenset (Asset)`/`Datensets (Assets)`:
  Da der Begriff in Airflow 3 neu eingeführt wurde steht er derzeit nicht fest.
  Daher eignet er sich zu der inhaltlich passenden Übersetzung. Um neue
  Benutzer nicht zu verwirren wird der durch Airflow definierte Originalterm in
  Klammern wenn möglich mitgeführt. Einzig in der Navigationsleiste ist der Begriff
  ohne die englische Referenz um den Text kurz zu halten.
- `Asset Event` --> `Ereignis zu Datenset (Asset)`: Logische Konsequenz der
  Übersetzung von -->"Asset" ohne einen sperrigen Begriff wie
  "Datenssatz-Ereignis" zu erzeugen.
- `Backfill` --> `Auffüllen`: Der technisch geprägte Term im Programm passt zu
  der direkten Übersetzung, auch im Deutschen werden Lücken wieder "aufgefüllt".
- `Bundle` --> `Bündel`: Die direkte Übersetzung passt zu dem Ziel der
  Englischen Begriffsdefinition.
- `Catchup` --> `Nachholen`: Direkte Übersetzung.
- `Connections` --> `Verbindungen`: Ist zwar ein feststehender Begriff in
  Airflow und ein technisches Konstrukt das im Code zu finden ist, jedoch
  direkt übersetzbar und erschließt sich damit neuen Benutzern direkt.
- `Dag ID`: Unübersetzt. "ID" sollte nach Duden fovorisiert groß geschrieben
  werden.
- `Dag Run` --> `Dag Lauf`: Auch wenn der Begriff Run im Code und in Logs oft
  zu finden ist, ist eine Eindeutschung für das Gesamtbild im UI vorteilhaft -
  abgesehen von dem feststehenden Begriff -->"Dag".
- `Deferred` (Status) --> `Delegiert`: Im Deutschen ist die Übersetzung nur
  teilweise passend und der Begriff "Delegiert" ist am nächsten an der
  Original-Bedeutung da ein Task zu der Triggerer-Komponente weitergegeben wird.
- `Docs` --> `Doku`: Die direkte Übersetzung wäre eigentlich "Dokumentation"
  gewesen aber ohne Wort-Trennung wäre der übersetzte Begriff einige Pixel zu
  breit für die Navigationsleiste. Deswegen wurde der im Deutschen auch
  gängige Begriff gewählt.
- `Map Index` --> `Planungs-Index`: Da es hier keine direkt passende Übersetzung
  gibt und der Begriff "Mapping" eigentlich übersetzbar ist - aber in dem
  genutzen Kontext irreführend wäre, wurde hier auf die Task-Planung verwiesen
  in der ein Task aufgeplant wird.
- `Operator` --> `Operator`: Da es sich hier um den vor allem mathematisch-
  technischen Begriff der Implementierung handelt, passt dieser Begriff am
  ehesten. Alternativen wie "Betreiber-Implementierung" sind sehr sperrig.
  Wir nutzen weiter den Begriff weil er im Programmcode sich auch so
  wiederfindet.
- `Plugins` --> `Plug-ins`: Nach Duden empfohlen.
- `Pools` (Unübersetzt): Der Englische Term ist so im Deutschen direkt
  verständlich. Ein präzise Übersetzung als "Ressourcen-Pool" wäre zu sperrig
  und andere Übersetzungsoptionen wie "Schwimmbad" irreführend. Insofern ist
  "Pool" als Kurzform von "Ressource-Pool" anzusehen.
- `Provider` (Unübersetzt): Eine direkte Übersetzung in andere Begriffe
  verbessert nicht das Verständnis. Der Term ist im Deutschen so gut
  verständlich.
- `Scheduled` --> `Geplant`: Vor allem genutzt für zyklisch laufende Dags.
- `Tag` --> `Markierung`: Kennzeichnen von Dags zur besseren Ordnung.
- `Task ID`: Unübersetzt. "ID" sollte nach Duden fovorisiert groß geschrieben
  werden.
- `Task Instance` --> `Task Instanz`: Der Begriff Task wird im Deutschen
  genutzt und passt zu der technischen Nutzung in Airflow. Alternativ wäre
  "Aufgabe" eine mögliche Übersetzung gewesen. Da aber der Begriff Task auch in
  Logs und Code zu finden ist, lag der Begriff etwas näher als "Aufgabe".
- `Trigger`(to) --> `Auslösen`: Genutzt für die Aktionen ein Lauf eines Dag zu
  starten. Von allen Optionen der am ehesten passende Begriff auch wenn es eine
  direkte Nutzung des Begriffs "Triggern" im Deutschen gibt. Der Begriff
  "Anstoßen" ist auch passend aber in dem Zusammenhan mit Trigger Rule zur
  Konsistenz ist "Auslösen" passender.
- `Trigger Rule` --> `Auslöse-Regel`: Im Ablauf eines Dags bestimmt die
  Auslöse-Regel (in Kombination mit der Position) jedes Tasks unter welchen
  Bedingungen und zu welchem Zeitpunkt im Dag Lauf dieser Task gestartet werden
  kann.
- `Try Number` --> `Versuch Nummer`: Direkt Übersetzung ist passend.
- `XCom` --> `Task Kommunikation (XCom)`: Um die Navigation für neue Benutzer
  verständlicher zu machen, wurde der Begriff sinngemäß auf Deutsch
  übersetzt. Zusätzlich ist XCom, da der Begriff oft in Airflow Code und Logs erscheint,
  nochmals in Klammern angegeben.

(Andere klassische Begriffsübersetzungen nicht im Einzelnen aufgeführt)
