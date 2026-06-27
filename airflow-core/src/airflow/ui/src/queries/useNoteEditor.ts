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
import { useState } from "react";

/**
 * Shared note-editing state and save logic, parameterised over the underlying
 * mutation. Keeps the local-state / diff-on-save / reset-on-open behaviour in
 * one place for the Dag run and task instance note hooks.
 */
export const useNoteEditor = ({
  isPending,
  mutateNote,
  savedNote,
}: {
  readonly isPending: boolean;
  readonly mutateNote: (note: string | null, options: { onError: () => void }) => void;
  readonly savedNote: string | null;
}) => {
  const [note, setNote] = useState<string | null>(savedNote);

  const onSave = () => {
    if (note !== savedNote) {
      mutateNote(note, { onError: () => setNote(savedNote ?? null) });
    }
  };

  // Reset local state to the server value each time an edit surface is opened,
  // so stale edits from a previous session don't linger.
  const onOpen = () => setNote(savedNote ?? "");

  return { isPending, note, onOpen, onSave, setNote };
};
