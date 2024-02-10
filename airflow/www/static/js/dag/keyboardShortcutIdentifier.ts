/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import type { KeyboardShortcutIdentifier } from "src/types";

const keyboardShortcutIdentifier: KeyboardShortcutIdentifier = {
  dagRunClear: {
    primaryKey: "shiftKey",
    secondaryKey: ["C", "c"],
    detail: "Clears the selected DAG run with all its existing tasks",
  },
  dagMarkSuccess: {
    primaryKey: "shiftKey",
    secondaryKey: ["S", "s"],
    detail: "Marks the selected DAG run as success",
  },
  dagMarkFailed: {
    primaryKey: "shiftKey",
    secondaryKey: ["F", "f"],
    detail: "Marks the selected DAG run as failed",
  },
  taskRunClear: {
    primaryKey: "shiftKey",
    secondaryKey: ["C", "c"],
    detail: "Opens modal to Clear selected task instance",
  },
  taskMarkSuccess: {
    primaryKey: "shiftKey",
    secondaryKey: ["S", "s"],
    detail: "Opens Mark as Success modal for the selected failed task instance",
  },
  taskMarkFailed: {
    primaryKey: "shiftKey",
    secondaryKey: ["F", "f"],
    detail:
      "Opens Mark as Failed modal for the selected successful task instance",
  },
  viewNotes: {
    primaryKey: "shiftKey",
    secondaryKey: ["N", "n"],
    detail: "View the note of the selected DAG run or Task instance",
  },
  addOrEditNotes: {
    primaryKey: "shiftKey",
    secondaryKey: ["E", "e"],
    detail: "Edit the note of the selected DAG run or Task instance",
  },
  toggleShortcutCheatSheet: {
    primaryKey: "shiftKey",
    secondaryKey: ["/", "?"],
    detail: "Toggle Shortcut cheat sheet",
  },
};

export default keyboardShortcutIdentifier;
